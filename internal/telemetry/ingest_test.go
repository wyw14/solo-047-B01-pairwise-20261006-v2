package telemetry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func sampleAt(id string, sequence uint64) Sample {
	return Sample{
		ID: id, AssetID: "R07", Kind: KindTemperature, Value: 42, Unit: "C",
		Sequence: sequence, Generation: 1, ObservedAt: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC),
	}
}

type recordingSink struct {
	mu       sync.Mutex
	fail     bool
	applied  []string
	attempts map[string]int
}

func (s *recordingSink) ApplySample(_ context.Context, sample Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = make(map[string]int)
	}
	s.attempts[sample.ID]++
	if s.fail {
		return errors.New("projection unavailable")
	}
	s.applied = append(s.applied, sample.ID)
	return nil
}

func (s *recordingSink) appliedCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[id]
}

func TestFailedProjectionKeepsSampleOutOfHistory(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{fail: true}
	ingestor := NewIngestor(repository, func() time.Time { return time.Now() }, sink)
	sample := sampleAt("sample-1", 1)

	if _, err := ingestor.Apply(context.Background(), sample); err == nil {
		t.Fatal("expected projection error")
	}
	if items := repository.History("R07"); len(items) != 0 {
		t.Fatalf("failed sample visible in history: %+v", items)
	}
	if state := repository.Status("sample-1"); state != SampleUnknown {
		t.Fatalf("failed sample state = %v, want unknown", state)
	}
}

func TestRetryAfterProjectionFailureSucceedsAndProjectsOnce(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{fail: true}
	ingestor := NewIngestor(repository, func() time.Time { return time.Now() }, sink)
	sample := sampleAt("sample-1", 1)

	if _, err := ingestor.Apply(context.Background(), sample); err == nil {
		t.Fatal("expected first attempt to fail")
	}
	sink.fail = false
	outcome, err := ingestor.Apply(context.Background(), sample)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if outcome.Replayed || outcome.Attempts != 2 {
		t.Fatalf("unexpected retry outcome: %+v", outcome)
	}
	items := repository.History("R07")
	if len(items) != 1 || items[0].ID != "sample-1" {
		t.Fatalf("history after retry: %+v", items)
	}
	if sink.appliedCount("sample-1") != 2 {
		t.Fatalf("sink invoked %d times, want 2 attempts", sink.appliedCount("sample-1"))
	}
	if len(sink.applied) != 1 || sink.applied[0] != "sample-1" {
		t.Fatalf("sample effectively projected %v, want exactly once", sink.applied)
	}
	if ingestor.Attempts("sample-1") != 2 {
		t.Fatalf("attempt history not trackable: %d", ingestor.Attempts("sample-1"))
	}
}

func TestConfirmedSampleIsNotProjectedAgain(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{}
	ingestor := NewIngestor(repository, func() time.Time { return time.Now() }, sink)
	sample := sampleAt("sample-1", 1)

	outcome, err := ingestor.Apply(context.Background(), sample)
	if err != nil || outcome.Replayed {
		t.Fatalf("first apply: %+v err=%v", outcome, err)
	}
	replay, err := ingestor.Apply(context.Background(), sample)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if !replay.Replayed || replay.Sample.ID != "sample-1" || replay.Attempts != 1 {
		t.Fatalf("unexpected replay outcome: %+v", replay)
	}
	if len(repository.History("R07")) != 1 {
		t.Fatal("replay duplicated the sample in history")
	}
	if len(sink.applied) != 1 {
		t.Fatalf("sink invoked %d times, want 1", len(sink.applied))
	}
}

func TestStagingRejectsConcurrentDuplicateAndAllowsRetry(t *testing.T) {
	repository := NewRepository()
	sample := sampleAt("sample-1", 1)
	if err := repository.Stage(sample); err != nil {
		t.Fatal(err)
	}
	if err := repository.Stage(sample); !errors.Is(err, ErrSamplePending) {
		t.Fatalf("duplicate pending stage = %v, want ErrSamplePending", err)
	}
	repository.Reject("sample-1")
	if err := repository.Stage(sample); err != nil {
		t.Fatalf("stage after reject failed: %v", err)
	}
	if _, err := repository.Commit("sample-1"); err != nil {
		t.Fatal(err)
	}
	err := repository.Stage(sampleAt("sample-1", 1))
	var duplicate *DuplicateSampleError
	if !errors.As(err, &duplicate) {
		t.Fatalf("restage confirmed = %v, want DuplicateSampleError", err)
	}
	if duplicate.Existing.ID != "sample-1" {
		t.Fatalf("duplicate error carried %v", duplicate.Existing.ID)
	}
	if _, err := repository.Commit("never-staged"); !errors.Is(err, ErrSampleNotStaged) {
		t.Fatalf("commit unknown = %v, want ErrSampleNotStaged", err)
	}
}
