package telemetry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type ProjectionSink interface {
	// ApplySample projects one confirmed-in-flight sample. Implementations MUST
	// be atomic: if ApplySample returns a non-nil error the observable
	// projection state must be unchanged, because the ingestion will be marked
	// failed and retried under the same key.
	ApplySample(context.Context, Sample) error
}

// IngestionLedger durably records the inbox lifecycle so that failed and
// retried ingestions stay trackable outside the in-memory repository.
type IngestionLedger interface {
	RecordReceived(context.Context, Ingestion) error
	RecordConfirmed(context.Context, Ingestion) error
	RecordFailed(context.Context, Ingestion) error
}

// IngestionError reports the stage at which an attempt stopped. Callers can
// inspect Stage and Status to map the failure onto a response and Err with
// errors.Is to recognise ErrInFlight / ErrIngestionConflict.
type IngestionError struct {
	Key      string
	SampleID string
	Attempts int
	Stage    IngestionStage
	// Status is the inbox state after the failure: failed for aborts before
	// confirmation, confirmed when the projection committed but the
	// confirmation acknowledgement could not be durably recorded.
	Status IngestionStatus
	Err    error
}

func (e *IngestionError) Error() string {
	return fmt.Sprintf("telemetry ingestion %q stopped at %s (attempt %d, status %s): %v",
		e.Key, e.Stage, e.Attempts, e.Status, e.Err)
}

func (e *IngestionError) Unwrap() error { return e.Err }

type Ingestor struct {
	repository *Repository
	ledger     IngestionLedger
	sinks      []ProjectionSink
	now        func() time.Time
}

func NewIngestor(repository *Repository, now func() time.Time, sinks ...ProjectionSink) *Ingestor {
	return &Ingestor{repository: repository, sinks: sinks, now: now}
}

// WithLedger attaches the durable inbox ledger used to trace received,
// confirmed and failed attempts.
func (i *Ingestor) WithLedger(ledger IngestionLedger) *Ingestor {
	i.ledger = ledger
	return i
}

// Ingest runs the receive → persist → project → confirm boundary for one
// sample under key (derive with IngestionKey when the caller has no explicit
// idempotency key).
//
// The sample only enters repository history once every sink succeeds and
// Confirm runs. A projection or persistence failure marks the ingestion
// failed, leaves it out of history and returns an IngestionError; retrying the
// same key resumes the tracked ingestion and increments Attempts. A retry of an
// already confirmed key reports replayed=true without touching any sink, so a
// valid sample is projected at most once.
func (i *Ingestor) Ingest(ctx context.Context, sample Sample, key string) (ingestion Ingestion, replayed bool, err error) {
	if sample.ReceivedAt.IsZero() {
		sample.ReceivedAt = i.now().UTC()
	}
	if key == "" {
		key = IngestionKey(sample)
	}

	ingestion, replayed, admitErr := i.repository.Admit(sample, key, i.now().UTC())
	if admitErr != nil {
		return Ingestion{Key: key, Sample: sample, Status: IngestionFailed}, false, &IngestionError{
			Key: key, SampleID: sample.ID, Attempts: 1,
			Stage: StageAdmit, Status: IngestionFailed, Err: admitErr,
		}
	}
	if replayed {
		return ingestion, true, nil
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			cause := fmt.Errorf("projection panicked: %v", recovered)
			ingestion, _, err = i.fail(ingestion, StageProjection, cause)
		}
	}()

	if i.ledger != nil {
		if ledgerErr := i.ledger.RecordReceived(ctx, ingestion); ledgerErr != nil {
			return i.fail(ingestion, StagePersist, fmt.Errorf("record inbound sample: %w", ledgerErr))
		}
	}

	for index, sink := range i.sinks {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return i.fail(ingestion, StageProjection, fmt.Errorf("sink %d cancelled before apply: %w", index, ctxErr))
		}
		if applyErr := sink.ApplySample(ctx, ingestion.Sample); applyErr != nil {
			return i.fail(ingestion, StageProjection, fmt.Errorf("sink %d (%T): %w", index, sink, applyErr))
		}
	}

	confirmed, _, confirmErr := i.repository.Confirm(ingestion.Key, i.now().UTC())
	if confirmErr != nil {
		return i.fail(ingestion, StageConfirm, confirmErr)
	}
	if i.ledger != nil {
		if ledgerErr := i.ledger.RecordConfirmed(ctx, confirmed); ledgerErr != nil {
			// The projection is committed and the sample is in history; only the
			// durable confirmation acknowledgement was lost. Keep the confirmed
			// state so a retry short-circuits as a replay instead of projecting
			// again.
			return confirmed, false, &IngestionError{
				Key: confirmed.Key, SampleID: confirmed.Sample.ID, Attempts: confirmed.Attempts,
				Stage: StageConfirm, Status: IngestionConfirmed,
				Err: fmt.Errorf("record confirmed sample: %w", ledgerErr),
			}
		}
	}
	return confirmed, false, nil
}

// fail moves an in-flight ingestion to the failed state, records the outcome
// in the durable ledger best-effort and builds the structured error.
func (i *Ingestor) fail(ingestion Ingestion, stage IngestionStage, cause error) (Ingestion, bool, error) {
	updated := i.repository.Fail(ingestion.Key, stage, cause, i.now().UTC())
	if i.ledger != nil {
		// A cancelled request context must not prevent the failure from being
		// recorded; the failure record is what makes the retry trackable.
		if ledgerErr := i.ledger.RecordFailed(context.WithoutCancel(context.Background()), updated); ledgerErr != nil {
			cause = errors.Join(cause, fmt.Errorf("record failed ingestion: %w", ledgerErr))
		}
	}
	return updated, false, &IngestionError{
		Key: updated.Key, SampleID: updated.Sample.ID, Attempts: updated.Attempts,
		Stage: stage, Status: IngestionFailed, Err: cause,
	}
}

// Apply keeps the legacy one-shot entry point for callers that do not need the
// idempotency key or the structured receipt.
func (i *Ingestor) Apply(ctx context.Context, sample Sample) error {
	_, _, err := i.Ingest(ctx, sample, "")
	return err
}
