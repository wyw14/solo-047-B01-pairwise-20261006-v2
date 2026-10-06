package telemetry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ProjectionSink consumes a confirmed sample. Implementations must be
// idempotent with respect to a sample ID: when an ingestion is retried after a
// failure, sinks that already observed the sample are invoked again and must
// not double-apply it.
type ProjectionSink interface {
	ApplySample(context.Context, Sample) error
}

// IngestOutcome makes the result of an ingestion attempt observable. Replayed
// is true when the sample ID was already confirmed and no projection ran.
// Attempts counts how many projection attempts (including failed ones) have
// been made for the sample ID, so a successful retry is distinguishable from
// a first-attempt success.
type IngestOutcome struct {
	Sample   Sample
	Replayed bool
	Attempts int
}

type Ingestor struct {
	repository *Repository
	sinks      []ProjectionSink
	now        func() time.Time

	attemptMu sync.Mutex
	attempts  map[string]int
}

func NewIngestor(repository *Repository, now func() time.Time, sinks ...ProjectionSink) *Ingestor {
	return &Ingestor{repository: repository, sinks: sinks, now: now, attempts: make(map[string]int)}
}

// Apply drives a sample across the receive/project/commit boundary:
//
//  1. The sample is staged in the repository (atomic test-and-set on its ID).
//  2. Every sink projects it.
//  3. Only when all sinks succeed is the sample committed and made visible in
//     history. On any projection error the sample is rejected, so a failed
//     sample can never be read downstream as confirmed data.
//
// A retry carrying an already-confirmed sample ID returns a replayed outcome
// without touching the sinks, so a valid sample is projected exactly once.
func (i *Ingestor) Apply(ctx context.Context, sample Sample) (IngestOutcome, error) {
	if sample.ReceivedAt.IsZero() {
		sample.ReceivedAt = i.now().UTC()
	}
	if err := i.repository.Stage(sample); err != nil {
		var duplicate *DuplicateSampleError
		if errors.As(err, &duplicate) {
			return IngestOutcome{
				Sample: duplicate.Existing, Replayed: true, Attempts: i.attemptCount(duplicate.Existing.ID),
			}, nil
		}
		return IngestOutcome{}, err
	}

	attempt := i.recordAttempt(sample.ID)

	var projectErr error
	for _, sink := range i.sinks {
		if err := sink.ApplySample(ctx, sample); err != nil {
			projectErr = fmt.Errorf("project telemetry sample: %w", err)
			break
		}
	}
	if projectErr != nil {
		// Abandon the staging slot: the sample stays invisible and the same ID
		// remains available for a tracked retry.
		i.repository.Reject(sample.ID)
		return IngestOutcome{Sample: sample, Attempts: attempt}, projectErr
	}

	committed, err := i.repository.Commit(sample.ID)
	if err != nil {
		return IngestOutcome{Sample: sample, Attempts: attempt}, fmt.Errorf("commit telemetry sample: %w", err)
	}
	return IngestOutcome{Sample: committed, Attempts: attempt}, nil
}

func (i *Ingestor) recordAttempt(id string) int {
	i.attemptMu.Lock()
	defer i.attemptMu.Unlock()
	i.attempts[id]++
	return i.attempts[id]
}

func (i *Ingestor) attemptCount(id string) int {
	i.attemptMu.Lock()
	defer i.attemptMu.Unlock()
	return i.attempts[id]
}

// Attempts reports how many projection attempts have been made for a sample
// ID, including failures. It exists to make retry histories inspectable.
func (i *Ingestor) Attempts(id string) int {
	return i.attemptCount(id)
}
