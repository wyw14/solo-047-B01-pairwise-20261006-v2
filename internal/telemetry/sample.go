package telemetry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// IngestionStage identifies the commit-boundary stage an attempt stopped at.
type IngestionStage string

const (
	StageAdmit      IngestionStage = "admit"
	StagePersist    IngestionStage = "persist"
	StageProjection IngestionStage = "projection"
	StageConfirm    IngestionStage = "confirm"
)

// IngestionStatus is the lifecycle state of an inbound sample.
type IngestionStatus string

const (
	// IngestionReceived means the sample passed validation and was admitted but
	// is not committed to history yet. Downstream readers must ignore it.
	IngestionReceived IngestionStatus = "received"
	// IngestionConfirmed means persistence and every projection succeeded and
	// the sample is part of the authoritative history.
	IngestionConfirmed IngestionStatus = "confirmed"
	// IngestionFailed means an attempt stopped at a stage; the sample stays out
	// of history and a retry under the same key is expected.
	IngestionFailed IngestionStatus = "failed"
)

// Sentinel errors used to classify admission outcomes with errors.Is.
var (
	// ErrInFlight is returned while a previous attempt for the same key is still
	// between admission and confirmation.
	ErrInFlight = errors.New("telemetry sample is already being processed")
	// ErrIngestionConflict is returned when a known key is retried with a sample
	// whose content differs from the original attempt.
	ErrIngestionConflict = errors.New("telemetry ingestion key reused with a different sample")
)

// Ingestion is the trackable inbox record for one inbound sample.
type Ingestion struct {
	Key        string          `json:"key"`
	Sample     Sample          `json:"sample"`
	Status     IngestionStatus `json:"status"`
	Attempts   int             `json:"attempts"`
	LastStage  IngestionStage  `json:"last_stage,omitempty"`
	LastError  string          `json:"last_error,omitempty"`
	ReceivedAt time.Time       `json:"received_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// IngestionKey derives the deterministic idempotency key for a sample: one
// physical observation from one asset of one kind at one sequence number.
func IngestionKey(sample Sample) string {
	return fmt.Sprintf("telemetry/%s/%s/%d", sample.AssetID, sample.Kind, sample.Sequence)
}

// sameSample compares the client-controlled observation fields. Server-stamped
// identity and timestamps (ID, ObservedAt/ReceivedAt) deliberately do not
// participate: a retried submission carries the same observation even though
// the server stamps it anew on each attempt.
func sameSample(a, b Sample) bool {
	return a.AssetID == b.AssetID &&
		a.Kind == b.Kind &&
		a.Sequence == b.Sequence &&
		a.Generation == b.Generation &&
		a.Value == b.Value &&
		a.Unit == b.Unit
}

type Repository struct {
	mu       sync.RWMutex
	samples  map[string][]Sample
	ingested map[string]*Ingestion
}

func NewRepository() *Repository {
	return &Repository{samples: make(map[string][]Sample), ingested: make(map[string]*Ingestion)}
}

// Admit reserves an inbound slot for key. It is idempotent across retries:
//
//   - an unknown key creates a received ingestion on attempt 1;
//   - a previously failed ingestion is re-armed as received and its attempt
//     counter advances, reusing the originally submitted sample;
//   - a confirmed ingestion is returned with replayed=true and no state change,
//     so retries never project twice;
//   - a received (in-flight) ingestion is rejected with ErrInFlight;
//   - a retry whose payload differs from the first attempt is rejected with
//     ErrIngestionConflict.
func (r *Repository) Admit(sample Sample, key string, at time.Time) (Ingestion, bool, error) {
	if err := sample.Validate(); err != nil {
		return Ingestion{}, false, err
	}
	if key == "" {
		key = IngestionKey(sample)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, exists := r.ingested[key]
	if exists {
		switch existing.Status {
		case IngestionConfirmed:
			if !sameSample(existing.Sample, sample) {
				return *existing, false, fmt.Errorf("%w: %s", ErrIngestionConflict, key)
			}
			return *existing, true, nil
		case IngestionReceived:
			return *existing, false, fmt.Errorf("%w: %s", ErrInFlight, key)
		case IngestionFailed:
			if !sameSample(existing.Sample, sample) {
				return *existing, false, fmt.Errorf("%w: %s", ErrIngestionConflict, key)
			}
			existing.Status = IngestionReceived
			existing.Attempts++
			existing.LastStage = ""
			existing.LastError = ""
			existing.UpdatedAt = at.UTC()
			return *existing, false, nil
		}
	}
	ingestion := &Ingestion{
		Key: key, Sample: sample, Status: IngestionReceived, Attempts: 1,
		ReceivedAt: at.UTC(), UpdatedAt: at.UTC(),
	}
	r.ingested[key] = ingestion
	return *ingestion, false, nil
}

// Fail marks an in-flight ingestion as failed at stage, recording the cause.
func (r *Repository) Fail(key string, stage IngestionStage, cause error, at time.Time) Ingestion {
	r.mu.Lock()
	defer r.mu.Unlock()
	ingestion, exists := r.ingested[key]
	if !exists || ingestion.Status != IngestionReceived {
		return Ingestion{Key: key, Status: IngestionFailed, LastStage: stage}
	}
	ingestion.Status = IngestionFailed
	ingestion.LastStage = stage
	if cause != nil {
		ingestion.LastError = cause.Error()
	}
	ingestion.UpdatedAt = at.UTC()
	return *ingestion
}

// Confirm commits an in-flight ingestion: its sample enters the authoritative
// history. Confirming an already confirmed ingestion is an idempotent replay.
func (r *Repository) Confirm(key string, at time.Time) (Ingestion, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ingestion, exists := r.ingested[key]
	if !exists {
		return Ingestion{Key: key}, false, fmt.Errorf("confirm unknown telemetry ingestion: %s", key)
	}
	if ingestion.Status == IngestionConfirmed {
		return *ingestion, true, nil
	}
	if ingestion.Status != IngestionReceived {
		return *ingestion, false, fmt.Errorf("confirm telemetry ingestion in status %q: %s", ingestion.Status, key)
	}
	ingestion.Status = IngestionConfirmed
	ingestion.LastStage = ""
	ingestion.LastError = ""
	ingestion.UpdatedAt = at.UTC()
	r.insertLocked(ingestion.Sample)
	return *ingestion, false, nil
}

// Lookup returns the tracked ingestion for key.
func (r *Repository) Lookup(key string) (Ingestion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ingestion, exists := r.ingested[key]
	if !exists {
		return Ingestion{}, false
	}
	return *ingestion, true
}

// Add appends a sample straight to confirmed history. It is intended for tests
// and recovery replay that bypasses the receive/projection pipeline.
func (r *Repository) Add(sample Sample) error {
	if err := sample.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.insertLocked(sample)
	return nil
}

func (r *Repository) insertLocked(sample Sample) {
	items := append(r.samples[sample.AssetID], sample)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Sequence == items[j].Sequence {
			return items[i].ObservedAt.Before(items[j].ObservedAt)
		}
		return items[i].Sequence < items[j].Sequence
	})
	r.samples[sample.AssetID] = items
}

type Kind string

const (
	KindOpticalPower Kind = "optical_power"
	KindErrorRate    Kind = "error_rate"
	KindCurrent      Kind = "current"
	KindTemperature  Kind = "temperature"
	KindHeartbeat    Kind = "heartbeat"
)

type Sample struct {
	ID         string    `json:"id"`
	AssetID    string    `json:"asset_id"`
	Kind       Kind      `json:"kind"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	Sequence   uint64    `json:"sequence"`
	Generation uint64    `json:"generation"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

func (s Sample) Validate() error {
	if s.ID == "" || s.AssetID == "" || s.Kind == "" {
		return errors.New("sample identity, asset and kind are required")
	}
	if s.Sequence == 0 || s.ObservedAt.IsZero() {
		return errors.New("sample sequence and observed time are required")
	}
	return nil
}

func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.samples[assetID]
	result := make([]Sample, len(items))
	copy(result, items)
	return result
}

func (r *Repository) Latest(assetID string, kind Kind) (Sample, bool) {
	items := r.History(assetID)
	for index := len(items) - 1; index >= 0; index-- {
		if items[index].Kind == kind {
			return items[index], true
		}
	}
	return Sample{}, false
}
