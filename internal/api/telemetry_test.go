package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

// flakySink fails the first projection of each sample ID, then succeeds. It
// records how many times each sample was effectively applied.
type flakySink struct {
	mu        sync.Mutex
	remaining int
	seen      []string
}

func (s *flakySink) ApplySample(_ context.Context, sample telemetry.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining > 0 {
		s.remaining--
		return errors.New("projector unavailable")
	}
	s.seen = append(s.seen, sample.ID)
	return nil
}

func (s *flakySink) applied() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func failingTelemetryServer(t *testing.T, failures int) (*Server, *telemetry.Repository, *flakySink) {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 10, 6, 3, 0, 0, 0, time.UTC)}
	repository := telemetry.NewRepository()
	asset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	sink := &flakySink{remaining: failures}
	ingestor := telemetry.NewIngestor(repository, clock.Now, sink)
	services := &Services{
		Clock: clock, IDs: platform.UUIDGenerator{},
		Journal:   store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Repeaters: map[string]*repeater.Repeater{"R07": asset},
		Telemetry: ingestor, TelemetryRepository: repository,
	}
	return NewServer(services), repository, sink
}

func postTelemetry(server *Server, key string, body TelemetryRequest) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	request := httptest.NewRequest(http.MethodPost, "/api/repeaters/R07/telemetry", bytes.NewReader(raw))
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestTelemetryRetryAfterProjectionFailureIsIdempotent(t *testing.T) {
	server, repository, sink := failingTelemetryServer(t, 1)
	body := TelemetryRequest{Kind: "temperature", Value: 40, Unit: "C", Sequence: 1, Generation: 1}

	// First attempt: persistence succeeds, projection fails -> 503, and the
	// unconfirmed sample must not be readable as history.
	first := postTelemetry(server, "retry-1", body)
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first ingest status=%d body=%s", first.Code, first.Body.String())
	}
	if items := repository.History("R07"); len(items) != 0 {
		t.Fatalf("failed sample readable as confirmed history: %+v", items)
	}

	// Retry with the same Idempotency-Key recovers the stored sample and
	// crosses the commit boundary exactly once.
	second := postTelemetry(server, "retry-1", body)
	if second.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", second.Code, second.Body.String())
	}
	var committed telemetry.Sample
	if err := json.Unmarshal(second.Body.Bytes(), &committed); err != nil {
		t.Fatal(err)
	}
	items := repository.History("R07")
	if len(items) != 1 || items[0].ID != committed.ID {
		t.Fatalf("history after retry = %+v, want single committed sample %s", items, committed.ID)
	}
	if applied := sink.applied(); len(applied) != 1 || applied[0] != committed.ID {
		t.Fatalf("sample effectively projected %v, want once as %s", applied, committed.ID)
	}

	// A third delivery of the same key is a pure replay: no duplicate row, no
	// second projection.
	third := postTelemetry(server, "retry-1", body)
	if third.Code != http.StatusOK {
		t.Fatalf("replay status=%d", third.Code)
	}
	if items := repository.History("R07"); len(items) != 1 {
		t.Fatalf("replay duplicated history: %+v", items)
	}
	if applied := sink.applied(); len(applied) != 1 {
		t.Fatalf("replay projected again: %v", applied)
	}
}

func TestTelemetryIdempotencyKeyRejectsDifferentPayload(t *testing.T) {
	server, _, _ := failingTelemetryServer(t, 0)
	body := TelemetryRequest{Kind: "temperature", Value: 40, Unit: "C", Sequence: 1, Generation: 1}
	if recorder := postTelemetry(server, "retry-2", body); recorder.Code != http.StatusAccepted {
		t.Fatalf("first ingest status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	body.Value = 99
	recorder := postTelemetry(server, "retry-2", body)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("conflicting retry status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestTelemetryWithoutKeyConfirmsOnFirstAttempt(t *testing.T) {
	server, repository, sink := failingTelemetryServer(t, 0)
	body := TelemetryRequest{Kind: "current", Value: 1.2, Unit: "A", Sequence: 3, Generation: 1}
	recorder := postTelemetry(server, "", body)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("ingest status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if items := repository.History("R07"); len(items) != 1 || items[0].Kind != telemetry.KindCurrent {
		t.Fatalf("unexpected history: %+v", items)
	}
	if applied := sink.applied(); len(applied) != 1 {
		t.Fatalf("sink applied %v, want 1", applied)
	}
}

func TestTelemetryRetryUsesSampleIDReturnedByFailedAttempt(t *testing.T) {
	server, repository, sink := failingTelemetryServer(t, 1)
	body := TelemetryRequest{Kind: "temperature", Value: 40, Unit: "C", Sequence: 9, Generation: 1}

	failed := postTelemetry(server, "", body)
	if failed.Code != http.StatusServiceUnavailable {
		t.Fatalf("first ingest status=%d body=%s", failed.Code, failed.Body.String())
	}
	var failure struct {
		SampleID string `json:"sample_id"`
		Key      string `json:"idempotency_key"`
	}
	if err := json.Unmarshal(failed.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.SampleID == "" || failure.Key != failure.SampleID {
		t.Fatalf("failure body must expose the retry handle: %+v", failure)
	}

	// Client retries using the returned sample ID as the idempotency key.
	retried := postTelemetry(server, failure.SampleID, body)
	if retried.Code != http.StatusOK {
		t.Fatalf("retry status=%d body=%s", retried.Code, retried.Body.String())
	}
	var committed telemetry.Sample
	if err := json.Unmarshal(retried.Body.Bytes(), &committed); err != nil {
		t.Fatal(err)
	}
	if committed.ID != failure.SampleID {
		t.Fatalf("retry created a new sample %s instead of reusing %s", committed.ID, failure.SampleID)
	}
	if items := repository.History("R07"); len(items) != 1 || items[0].ID != failure.SampleID {
		t.Fatalf("retry produced duplicate inbound samples: %+v", items)
	}
	if applied := sink.applied(); len(applied) != 1 || applied[0] != failure.SampleID {
		t.Fatalf("sample projected %v, want once as %s", applied, failure.SampleID)
	}
}
