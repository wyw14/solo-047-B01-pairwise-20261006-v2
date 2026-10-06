package telemetry

import (
	"errors"
	"sort"
	"sync"
	"time"
)

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

// SampleState tracks the commit boundary of an inbound sample. Only confirmed
// samples are ever visible to downstream readers.
type SampleState int

const (
	SampleUnknown SampleState = iota
	SamplePending
	SampleConfirmed
)

var (
	// ErrSamplePending is returned when a sample ID already has an in-flight
	// ingestion attempt.
	ErrSamplePending = errors.New("telemetry sample ingestion is already in flight")
	// ErrSampleNotStaged is returned when committing or rejecting a sample
	// that was never staged.
	ErrSampleNotStaged = errors.New("telemetry sample is not staged")
)

// DuplicateSampleError reports that a sample ID has already been confirmed. It
// carries the original sample so retries can be resolved idempotently instead
// of being projected a second time.
type DuplicateSampleError struct {
	Existing Sample
}

func (e *DuplicateSampleError) Error() string {
	return "telemetry sample already confirmed: " + e.Existing.ID
}

// Repository is the commit boundary between reception and projection. A
// sample moves through three phases:
//
//	unknown --Stage--> pending --Commit--> confirmed
//	                       └────Reject────> unknown
//
// History, Latest and CurrentSamples only ever expose confirmed samples, so a
// sample whose projection failed is never observable by downstream consumers
// as if it had been acknowledged.
type Repository struct {
	mu          sync.RWMutex
	confirmed   map[string][]Sample
	pending     map[string]Sample
	confirmedID map[string]string
}

func NewRepository() *Repository {
	return &Repository{
		confirmed:   make(map[string][]Sample),
		pending:     make(map[string]Sample),
		confirmedID: make(map[string]string),
	}
}

// Stage validates a sample and reserves its ID for an in-flight ingestion. It
// is an atomic test-and-set: a confirmed ID yields a *DuplicateSampleError so
// the caller can short-circuit a retry, and a pending ID yields
// ErrSamplePending so concurrent attempts cannot double-project.
func (r *Repository) Stage(sample Sample) error {
	if err := sample.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.pending[sample.ID]; exists {
		return ErrSamplePending
	}
	if assetID, exists := r.confirmedID[sample.ID]; exists {
		if existing, ok := r.findLocked(assetID, sample.ID); ok {
			return &DuplicateSampleError{Existing: existing}
		}
	}
	r.pending[sample.ID] = sample
	return nil
}

// Commit promotes a staged sample into confirmed history. It returns
// ErrSampleNotStaged when the sample was rejected (or never staged).
func (r *Repository) Commit(id string) (Sample, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sample, ok := r.pending[id]
	if !ok {
		return Sample{}, ErrSampleNotStaged
	}
	delete(r.pending, id)
	r.insertConfirmedLocked(sample)
	r.confirmedID[id] = sample.AssetID
	return sample, nil
}

// Reject abandons a staged sample after a projection failure so it is neither
// visible in history nor blocked from a later retry.
func (r *Repository) Reject(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.pending, id)
}

// Status reports where a sample sits relative to the commit boundary.
func (r *Repository) Status(id string) SampleState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.pending[id]; ok {
		return SamplePending
	}
	if _, ok := r.confirmedID[id]; ok {
		return SampleConfirmed
	}
	return SampleUnknown
}

// Confirmed returns a previously confirmed sample by ID.
func (r *Repository) Confirmed(id string) (Sample, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	assetID, ok := r.confirmedID[id]
	if !ok {
		return Sample{}, false
	}
	return r.findLocked(assetID, id)
}

func (r *Repository) findLocked(assetID, id string) (Sample, bool) {
	for _, candidate := range r.confirmed[assetID] {
		if candidate.ID == id {
			return candidate, true
		}
	}
	return Sample{}, false
}

func (r *Repository) insertConfirmedLocked(sample Sample) {
	items := append(r.confirmed[sample.AssetID], sample)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Sequence == items[j].Sequence {
			return items[i].ObservedAt.Before(items[j].ObservedAt)
		}
		return items[i].Sequence < items[j].Sequence
	})
	r.confirmed[sample.AssetID] = items
}

// Add confirms a sample directly without going through the staging boundary.
// It is intended for tooling and tests that populate history outside the
// receive/project pipeline; ingestion must use Stage/Commit instead.
func (r *Repository) Add(sample Sample) error {
	if err := sample.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.insertConfirmedLocked(sample)
	r.confirmedID[sample.ID] = sample.AssetID
	return nil
}

func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.confirmed[assetID]
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
