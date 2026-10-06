package api

import (
	"context"
	"time"

	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

// Journal kinds emitted per inbox transition. Failures stay queryable via
// /api/events?kind=telemetry-failed and every attempt for one observation
// clusters under the same entity_id (the idempotency key), so retries are
// traceable end to end.
const (
	ledgerReceivedKind  = "telemetry-received"
	ledgerConfirmedKind = "telemetry-confirmed"
	ledgerFailedKind    = "telemetry-failed"
)

// telemetryLedger adapts the durable event journal to telemetry.IngestionLedger.
// The record payload is the full inbox state (status/attempts/last error).
type telemetryLedger struct {
	journal *store.Journal
	now     func() time.Time
}

func NewTelemetryLedger(journal *store.Journal, now func() time.Time) telemetry.IngestionLedger {
	return &telemetryLedger{journal: journal, now: now}
}

func (l *telemetryLedger) append(ctx context.Context, kind string, ingestion telemetry.Ingestion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return l.journal.Append(kind, ingestion.Key, ingestion, l.now().UTC())
}

func (l *telemetryLedger) RecordReceived(ctx context.Context, ingestion telemetry.Ingestion) error {
	return l.append(ctx, ledgerReceivedKind, ingestion)
}

func (l *telemetryLedger) RecordConfirmed(ctx context.Context, ingestion telemetry.Ingestion) error {
	return l.append(ctx, ledgerConfirmedKind, ingestion)
}

func (l *telemetryLedger) RecordFailed(_ context.Context, ingestion telemetry.Ingestion) error {
	// Failure is recorded with a fresh background context in the ingestor, so
	// a disconnected client does not erase the audit trail of the attempt.
	return l.journal.Append(ledgerFailedKind, ingestion.Key, ingestion, l.now().UTC())
}
