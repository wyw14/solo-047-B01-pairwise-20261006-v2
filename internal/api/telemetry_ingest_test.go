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

// failingFirstSink fails its first failingCalls ApplySample invocations.
type failingFirstSink struct {
	mu           sync.Mutex
	calls        int
	failingCalls int
}

func (s *failingFirstSink) ApplySample(context.Context, telemetry.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failingCalls {
		return errors.New("health projection backend unavailable")
	}
	return nil
}

func ingestBoundaryServer(t *testing.T, sink telemetry.ProjectionSink) (*Server, *store.Journal, *telemetry.Repository) {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 10, 5, 4, 0, 0, 0, time.UTC)}
	journal := store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl"))
	repository := telemetry.NewRepository()
	asset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	ingestor := telemetry.NewIngestor(repository, clock.Now, sink).
		WithLedger(NewTelemetryLedger(journal, clock.Now))
	services := &Services{
		Clock: clock, IDs: platform.UUIDGenerator{}, Journal: journal,
		Repeaters: map[string]*repeater.Repeater{"R07": asset},
		Telemetry: ingestor, TelemetryRepository: repository,
	}
	return NewServer(services), journal, repository
}

func postTelemetry(t *testing.T, server *Server, body []byte, key string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/repeaters/R07/telemetry", bytes.NewReader(body))
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestTelemetryIngestionFailureThenRetryBoundaries(t *testing.T) {
	server, journal, repository := ingestBoundaryServer(t, &failingFirstSink{failingCalls: 1})

	payload := []byte(`{"kind":"current","value":12.5,"unit":"A","sequence":1,"generation":3}`)

	// First attempt: projection fails. It must be reported as a trackable
	// upstream failure and the sample must not be visible as confirmed data.
	failed := postTelemetry(t, server, payload, "obs-R07-current-1")
	if failed.Code != http.StatusBadGateway {
		t.Fatalf("first attempt status=%d body=%s", failed.Code, failed.Body.String())
	}
	var failureBody struct {
		Error       string `json:"error"`
		IngestionID string `json:"ingestion_id"`
		Stage       string `json:"stage"`
		Attempts    int    `json:"attempts"`
	}
	if err := json.Unmarshal(failed.Body.Bytes(), &failureBody); err != nil {
		t.Fatal(err)
	}
	if failureBody.Stage != "projection" || failureBody.IngestionID != "obs-R07-current-1" || failureBody.Attempts != 1 {
		t.Fatalf("failure receipt not trackable: %+v", failureBody)
	}
	if history := repository.History("R07"); len(history) != 0 {
		t.Fatalf("failed sample visible as confirmed history: %+v", history)
	}

	// Retry with the same idempotency key: succeeds on attempt 2.
	retry := postTelemetry(t, server, payload, "obs-R07-current-1")
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	var ack TelemetryAck
	if err := json.Unmarshal(retry.Body.Bytes(), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Status != telemetry.IngestionConfirmed || ack.Replayed || ack.Attempts != 2 {
		t.Fatalf("retry ack wrong: %+v", ack)
	}
	if history := repository.History("R07"); len(history) != 1 || history[0].Sequence != 1 {
		t.Fatalf("valid sample must be projected exactly once: %+v", history)
	}

	// Third delivery of the same observation is a replay: no re-projection.
	replay := postTelemetry(t, server, payload, "obs-R07-current-1")
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &ack); err != nil {
		t.Fatal(err)
	}
	if !ack.Replayed || ack.Attempts != 2 {
		t.Fatalf("replay ack wrong: %+v", ack)
	}
	if history := repository.History("R07"); len(history) != 1 {
		t.Fatalf("replay duplicated history: %+v", history)
	}

	// The full attempt trail is durable in the journal.
	counts := map[string]int{}
	for _, kind := range []string{ledgerReceivedKind, ledgerConfirmedKind, ledgerFailedKind} {
		page, err := readEvents(journal, kind)
		if err != nil {
			t.Fatal(err)
		}
		counts[kind] = page.Total
	}
	if counts[ledgerReceivedKind] != 2 || counts[ledgerConfirmedKind] != 1 || counts[ledgerFailedKind] != 1 {
		t.Fatalf("durable attempt trail wrong: %+v", counts)
	}
}

func TestTelemetryDerivesKeyAndRejectsInvalidSample(t *testing.T) {
	server, _, repository := ingestBoundaryServer(t, &failingFirstSink{})

	// Missing sequence fails validation at admit with 400 and records nothing.
	invalid := postTelemetry(t, server, []byte(`{"kind":"current","value":1,"unit":"A","sequence":0}`), "")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid sample status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	if len(repository.History("R07")) != 0 {
		t.Fatal("invalid sample entered history")
	}

	// Two deliveries without an explicit key derive the same asset/kind/seq key,
	// so the second is a replay rather than a duplicate inbound.
	payload := []byte(`{"kind":"temperature","value":40,"unit":"C","sequence":5,"generation":1}`)
	first := postTelemetry(t, server, payload, "")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := postTelemetry(t, server, payload, "")
	if second.Code != http.StatusOK {
		t.Fatalf("derived-key replay status=%d body=%s", second.Code, second.Body.String())
	}
	if history := repository.History("R07"); len(history) != 1 {
		t.Fatalf("derived key failed to dedupe: %+v", history)
	}
}

func readEvents(journal *store.Journal, kind string) (EventPage, error) {
	records, err := journal.Records()
	if err != nil {
		return EventPage{}, err
	}
	page := EventPage{}
	for _, record := range records {
		if record.Kind == kind {
			page.Total++
		}
	}
	return page, nil
}
