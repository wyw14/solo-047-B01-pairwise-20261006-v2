package telemetry

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func testSample(sequence uint64) Sample {
	return Sample{
		ID: "sample-" + strconv.FormatUint(sequence, 10), AssetID: "R07", Kind: KindCurrent,
		Value: float64(sequence), Unit: "A", Sequence: sequence,
		ObservedAt: time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC),
	}
}

// recordingLedger captures durable inbox transitions and can script failures.
type recordingLedger struct {
	mu          sync.Mutex
	received    []Ingestion
	confirmed   []Ingestion
	failed      []Ingestion
	failReceive bool
	failConfirm bool
}

func (l *recordingLedger) snapshot() (int, int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.received), len(l.confirmed), len(l.failed)
}

func (l *recordingLedger) RecordReceived(_ context.Context, ingestion Ingestion) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.received = append(l.received, ingestion)
	if l.failReceive {
		return errors.New("journal unavailable")
	}
	return nil
}

func (l *recordingLedger) RecordConfirmed(_ context.Context, ingestion Ingestion) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.confirmed = append(l.confirmed, ingestion)
	if l.failConfirm {
		return errors.New("journal unavailable")
	}
	return nil
}

func (l *recordingLedger) RecordFailed(_ context.Context, ingestion Ingestion) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failed = append(l.failed, ingestion)
	return nil
}

// flakySink fails its first failingCalls invocations, then succeeds.
type flakySink struct {
	mu           sync.Mutex
	calls        int
	failingCalls int
}

func (s *flakySink) ApplySample(_ context.Context, _ Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failingCalls {
		return errors.New("projection backend unavailable")
	}
	return nil
}

func (s *flakySink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type countingSink struct{ calls int }

func (s *countingSink) ApplySample(_ context.Context, _ Sample) error {
	s.calls++
	return nil
}

// gateSink blocks inside ApplySample until released, holding the in-flight slot.
type gateSink struct {
	entered chan struct{}
	release chan struct{}
}

func (s *gateSink) ApplySample(context.Context, Sample) error {
	s.entered <- struct{}{}
	<-s.release
	return nil
}

func TestIngestHappyPathCommitsOnce(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ledger := &recordingLedger{}
	ingestor := NewIngestor(repository, func() time.Time { return baseTime() }, sink).WithLedger(ledger)

	ingestion, replayed, err := ingestor.Ingest(context.Background(), testSample(1), "")
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if replayed || ingestion.Status != IngestionConfirmed || ingestion.Attempts != 1 {
		t.Fatalf("unexpected receipt: %+v replayed=%v", ingestion, replayed)
	}
	if sink.calls != 1 {
		t.Fatalf("sink invoked %d times, want 1", sink.calls)
	}
	if history := repository.History("R07"); len(history) != 1 || history[0].Sequence != 1 {
		t.Fatalf("confirmed history wrong: %+v", history)
	}
	if got, confirmed, failed := ledger.snapshot(); got != 1 || confirmed != 1 || failed != 0 {
		t.Fatalf("ledger transitions received=%d confirmed=%d failed=%d", got, confirmed, failed)
	}
}

func TestIngestProjectionFailureKeepsSampleOutOfHistoryAndRetryProjectsOnce(t *testing.T) {
	repository := NewRepository()
	sink := &flakySink{failingCalls: 1}
	ledger := &recordingLedger{}
	clock := newFakeClock()
	ingestor := NewIngestor(repository, clock.now, sink).WithLedger(ledger)
	key := IngestionKey(testSample(1))

	_, _, firstErr := ingestor.Ingest(context.Background(), testSample(1), key)
	if firstErr == nil {
		t.Fatal("expected projection error")
	}
	var ingestionErr *IngestionError
	if !errors.As(firstErr, &ingestionErr) || ingestionErr.Stage != StageProjection {
		t.Fatalf("want projection IngestionError, got %#v", firstErr)
	}
	if history := repository.History("R07"); len(history) != 0 {
		t.Fatalf("failed sample leaked into history: %+v", history)
	}
	tracked, ok := repository.Lookup(key)
	if !ok || tracked.Status != IngestionFailed || tracked.Attempts != 1 ||
		tracked.LastStage != StageProjection || tracked.LastError == "" {
		t.Fatalf("failed ingestion not tracked: %+v ok=%v", tracked, ok)
	}
	if _, _, failed := ledger.snapshot(); failed != 1 {
		t.Fatalf("want 1 durable failure record, got %d", failed)
	}

	// Retry the same observation under the same key: it resumes the tracked
	// ingestion instead of creating a duplicate.
	ingestion, replayed, retryErr := ingestor.Ingest(context.Background(), testSample(1), key)
	if retryErr != nil {
		t.Fatalf("retry ingest: %v", retryErr)
	}
	if replayed {
		t.Fatal("retry of a failed ingestion must run again, not report a replay")
	}
	if ingestion.Status != IngestionConfirmed || ingestion.Attempts != 2 {
		t.Fatalf("retry receipt wrong: %+v", ingestion)
	}
	if history := repository.History("R07"); len(history) != 1 || history[0].Sequence != 1 {
		t.Fatalf("history must hold the sample exactly once: %+v", history)
	}
	// One failed attempt plus one successful attempt reached the sink, but the
	// sample took effect in the projection only once.
	if sink.callCount() != 2 {
		t.Fatalf("sink invoked %d times, want 2 (1 failed + 1 effective)", sink.callCount())
	}
	if received, confirmed, failed := ledger.snapshot(); received != 2 || confirmed != 1 || failed != 1 {
		t.Fatalf("ledger trail wrong received=%d confirmed=%d failed=%d", received, confirmed, failed)
	}
}

func TestIngestConfirmedReplayShortCircuitsSinks(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ingestor := NewIngestor(repository, func() time.Time { return baseTime() }, sink)
	key := IngestionKey(testSample(2))
	if _, _, err := ingestor.Ingest(context.Background(), testSample(2), key); err != nil {
		t.Fatal(err)
	}

	ingestion, replayed, err := ingestor.Ingest(context.Background(), testSample(2), key)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replayed || ingestion.Status != IngestionConfirmed || ingestion.Attempts != 1 {
		t.Fatalf("replay receipt wrong: %+v replayed=%v", ingestion, replayed)
	}
	if sink.calls != 1 {
		t.Fatalf("replay re-projected the sample, sink calls=%d", sink.calls)
	}
	if history := repository.History("R07"); len(history) != 1 {
		t.Fatalf("replay duplicated history: %+v", history)
	}
}

func TestIngestPersistenceFailurePreventsProjection(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ledger := &recordingLedger{failReceive: true}
	ingestor := NewIngestor(repository, func() time.Time { return baseTime() }, sink).WithLedger(ledger)
	key := IngestionKey(testSample(3))

	_, _, err := ingestor.Ingest(context.Background(), testSample(3), key)
	var ingestionErr *IngestionError
	if !errors.As(err, &ingestionErr) || ingestionErr.Stage != StagePersist {
		t.Fatalf("want persist-stage error, got %#v", err)
	}
	if sink.calls != 0 {
		t.Fatal("sink ran despite persistence failure")
	}
	if history := repository.History("R07"); len(history) != 0 {
		t.Fatalf("unpersisted sample leaked into history: %+v", history)
	}

	// Repair the ledger and retry: the sample now projects exactly once.
	ledger.failReceive = false
	ingestion, _, err := ingestor.Ingest(context.Background(), testSample(3), key)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if ingestion.Status != IngestionConfirmed || ingestion.Attempts != 2 {
		t.Fatalf("retry receipt wrong: %+v", ingestion)
	}
	if sink.calls != 1 || len(repository.History("R07")) != 1 {
		t.Fatalf("after repair sink calls=%d history=%d", sink.calls, len(repository.History("R07")))
	}
}

func TestIngestConcurrentAttemptForInFlightKeyRejected(t *testing.T) {
	repository := NewRepository()
	gate := &gateSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	ingestor := NewIngestor(repository, func() time.Time { return baseTime() }, gate)
	key := IngestionKey(testSample(4))

	done := make(chan error, 1)
	go func() {
		_, _, err := ingestor.Ingest(context.Background(), testSample(4), key)
		done <- err
	}()
	<-gate.entered

	_, _, err := ingestor.Ingest(context.Background(), testSample(4), key)
	if !errors.Is(err, ErrInFlight) {
		t.Fatalf("want ErrInFlight, got %v", err)
	}
	close(gate.release)
	if err := <-done; err != nil {
		t.Fatalf("held ingestion: %v", err)
	}
	if len(repository.History("R07")) != 1 {
		t.Fatalf("history wrong after release: %+v", repository.History("R07"))
	}
}

func TestIngestSameKeyWithDifferentSampleConflicts(t *testing.T) {
	repository := NewRepository()
	sink := &flakySink{failingCalls: 1}
	ingestor := NewIngestor(repository, func() time.Time { return baseTime() }, sink)
	key := IngestionKey(testSample(5))
	if _, _, err := ingestor.Ingest(context.Background(), testSample(5), key); err == nil {
		t.Fatal("expected first attempt to fail")
	}
	conflicting := testSample(5)
	conflicting.Value = 999
	if _, _, err := ingestor.Ingest(context.Background(), conflicting, key); !errors.Is(err, ErrIngestionConflict) {
		t.Fatalf("want ErrIngestionConflict, got %v", err)
	}
}

func TestIngestConfirmationLedgerFailureStaysConfirmedAndReplayIsNoop(t *testing.T) {
	repository := NewRepository()
	sink := &countingSink{}
	ledger := &recordingLedger{failConfirm: true}
	ingestor := NewIngestor(repository, func() time.Time { return baseTime() }, sink).WithLedger(ledger)
	key := IngestionKey(testSample(6))

	_, _, err := ingestor.Ingest(context.Background(), testSample(6), key)
	var ingestionErr *IngestionError
	if !errors.As(err, &ingestionErr) || ingestionErr.Status != IngestionConfirmed {
		t.Fatalf("want confirmed-status acknowledgement error, got %#v", err)
	}
	if history := repository.History("R07"); len(history) != 1 {
		t.Fatalf("sample must already be in history: %+v", history)
	}

	// Repair the ledger; the retry must short-circuit as a replay and never
	// re-project.
	ledger.failConfirm = false
	ingestion, replayed, err := ingestor.Ingest(context.Background(), testSample(6), key)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !replayed || ingestion.Status != IngestionConfirmed {
		t.Fatalf("want confirmed replay, got %+v replayed=%v", ingestion, replayed)
	}
	if sink.calls != 1 {
		t.Fatalf("replay re-projected after confirm failure, calls=%d", sink.calls)
	}
}

type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock { return &fakeClock{t: baseTime()} }
func (c *fakeClock) now() time.Time {
	c.t = c.t.Add(time.Second)
	return c.t.UTC()
}

func baseTime() time.Time { return time.Date(2026, 10, 5, 2, 0, 0, 0, time.UTC) }
