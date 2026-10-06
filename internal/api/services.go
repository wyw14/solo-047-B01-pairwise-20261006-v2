package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

// ErrIdempotencyConflict is returned when an idempotency key is reused for a
// different sample payload.
var ErrIdempotencyConflict = errors.New("idempotency key was already used for a different telemetry sample")

type Services struct {
	Clock               platform.Clock
	IDs                 platform.IDGenerator
	Journal             *store.Journal
	Spans               map[string]*span.Span
	Routes              map[string]*route.Route
	Repeaters           map[string]*repeater.Repeater
	Topology            *topology.Registry
	OTDR                *otdr.SessionService
	Switchovers         *switchover.SessionService
	Permits             *repair.PermitService
	Receipts            *landing.ReceiptRegistry
	Acceptance          *repair.AcceptanceService
	Telemetry           *telemetry.Ingestor
	TelemetryRepository *telemetry.Repository
	Admission           *incident.AdmissionService
	Classifier          *incident.Classifier
	Correlator          *otdr.Correlator
	Health              *landing.HealthService
	Station             *landing.Station
	WetPlantTests       *repair.TestService
	Ramps               *repeater.RampController
	Assets              *repair.Assets
	RepeaterHealth      *repeater.HealthProjector
	Actuator            *feed.Actuator
	Tests               map[string]*repair.TestSession
	Incidents           map[string]*incident.Incident
	mu                  sync.RWMutex
}

func (s *Services) Span(id string) (*span.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Spans[id]
	if target == nil {
		return nil, fmt.Errorf("span %s not found", id)
	}
	return target, nil
}

func (s *Services) Route(id string) (*route.Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Routes[id]
	if target == nil {
		return nil, fmt.Errorf("route %s not found", id)
	}
	return target, nil
}

func (s *Services) Repeater(id string) (*repeater.Repeater, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	target := s.Repeaters[id]
	if target == nil {
		return nil, fmt.Errorf("repeater %s not found", id)
	}
	return target, nil
}

func (s *Services) Persist(kind, entityID string, value any) error {
	return s.Journal.Append(kind, entityID, value, s.Clock.Now())
}

func (s *Services) StartWetPlantTest(ctx context.Context, spanID string) repair.TestSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	session := s.WetPlantTests.Start(ctx, spanID)
	snapshot := s.WetPlantTests.Snapshot(session)
	s.Tests[snapshot.ID] = session
	return snapshot
}

func (s *Services) FinishWetPlantTest(id, action string) (repair.TestSnapshot, error) {
	s.mu.Lock()
	session := s.Tests[id]
	delete(s.Tests, id)
	s.mu.Unlock()
	if session == nil {
		return repair.TestSnapshot{}, fmt.Errorf("wet-plant test %s not found", id)
	}
	if action == "complete" {
		return s.WetPlantTests.Complete(session), nil
	}
	return s.WetPlantTests.Cancel(session), nil
}

func (s *Services) Incident(id, spanID string) *incident.Incident {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.Incidents[id]
	if current == nil {
		current = incident.New(id, spanID, s.Clock.Now().Add(10*time.Second), s.Clock.Now())
		s.Incidents[id] = current
	}
	return current
}

func (s *Services) TelemetryHistory(assetID string) []telemetry.Sample {
	items := s.TelemetryRepository.History(assetID)
	if latest, ok := s.TelemetryRepository.Latest(assetID, telemetry.KindTemperature); ok {
		_ = latest
	}
	return items
}

// IngestTelemetryResult makes the receive/persist/project boundary observable:
// Stored is false on an idempotent replay, Replayed is true when the sample
// was already projected before this call, and Attempts counts projection
// attempts for the sample ID (failures included).
type IngestTelemetryResult struct {
	Sample   telemetry.Sample
	Stored   bool
	Replayed bool
	Attempts int
}

// IngestTelemetry fixes the ordering of reception, persistence and
// projection. The sample is persisted to the durable journal BEFORE any sink
// runs and the append is idempotent on the sample ID. A failed projection
// therefore never loses the inbound sample, and a retry with the same
// idempotency key recovers the originally persisted sample instead of
// creating a duplicate.
func (s *Services) IngestTelemetry(ctx context.Context, key string, sample telemetry.Sample) (IngestTelemetryResult, error) {
	if err := sample.Validate(); err != nil {
		return IngestTelemetryResult{}, err
	}
	stored, err := s.Journal.AppendIfAbsent("telemetry", key, sample, s.Clock.Now().UTC())
	if err != nil {
		return IngestTelemetryResult{}, fmt.Errorf("persist telemetry sample: %w", err)
	}
	if !stored {
		record, found, err := s.Journal.Record("telemetry", key)
		if err != nil {
			return IngestTelemetryResult{}, fmt.Errorf("recover persisted telemetry sample: %w", err)
		}
		if !found {
			return IngestTelemetryResult{}, errors.New("persisted telemetry record not found")
		}
		var original telemetry.Sample
		if err := json.Unmarshal(record.Payload, &original); err != nil {
			return IngestTelemetryResult{}, fmt.Errorf("decode persisted telemetry sample: %w", err)
		}
		if !sameTelemetryPayload(original, sample) {
			return IngestTelemetryResult{}, ErrIdempotencyConflict
		}
		sample = original
	}
	outcome, err := s.Telemetry.Apply(ctx, sample)
	if err != nil {
		return IngestTelemetryResult{Sample: sample, Stored: stored}, err
	}
	return IngestTelemetryResult{
		Sample:   outcome.Sample,
		Stored:   stored,
		Replayed: outcome.Replayed,
		Attempts: outcome.Attempts,
	}, nil
}

// sameTelemetryPayload compares the client-supplied fields. ObservedAt and
// ReceivedAt are server stamps that legitimately differ between the original
// attempt and a retry.
func sameTelemetryPayload(left, right telemetry.Sample) bool {
	return left.AssetID == right.AssetID &&
		left.Kind == right.Kind &&
		left.Unit == right.Unit &&
		left.Sequence == right.Sequence &&
		left.Generation == right.Generation &&
		left.Value == right.Value
}
