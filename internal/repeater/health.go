package repeater

import (
	"context"
	"fmt"
	"sync"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type IncidentSink interface {
	OpenRepeaterIncident(repeaterID string, generation uint64) error
	ResolveRepeaterIncident(repeaterID string, generation uint64) error
	Active(repeaterID string) (uint64, bool)
	LastResolution(repeaterID string) uint64
}

type HealthProjector struct {
	mu        sync.RWMutex
	repeaters map[string]*Repeater
	incidents IncidentSink
	tripAtC   float64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{repeaters: make(map[string]*Repeater), incidents: incidents, tripAtC: tripAtC}
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = repeater
}

// ApplySample is atomic: it computes the next snapshot, performs the incident
// side effect first, and only commits the repeater's projection fields once
// every side effect succeeded. On error the observable state is unchanged, so
// the ingestor can mark the ingestion failed and retry the same key without
// double-applying.
func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if sample.Kind != telemetry.KindTemperature {
		return nil
	}
	p.mu.RLock()
	repeater := p.repeaters[sample.AssetID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	before := repeater.Snapshot()
	if sample.Sequence <= before.LastSequence || sample.Generation < before.TripGeneration {
		return nil
	}

	after := before
	after.LastSequence = sample.Sequence
	after.TemperatureC = sample.Value
	after.UpdatedAt = sample.ObservedAt.UTC()
	trips := false
	if sample.Value >= p.tripAtC {
		after.Health = HealthTripped
		after.TripGeneration = sample.Generation
		trips = true
	} else if before.Health != HealthTripped {
		after.Health = HealthNormal
	}

	// External side effect happens before the local state commit. If it fails
	// the repeater snapshot stays at `before` and the ingestion is retried.
	if trips && before.Health != HealthTripped && p.incidents != nil {
		if err := p.incidents.OpenRepeaterIncident(after.ID, after.TripGeneration); err != nil {
			return fmt.Errorf("open repeater incident: %w", err)
		}
	}

	committed := repeater.CommitProjected(before, func(current *Snapshot) {
		current.LastSequence = after.LastSequence
		current.TemperatureC = after.TemperatureC
		current.UpdatedAt = after.UpdatedAt
		current.Health = after.Health
		current.TripGeneration = after.TripGeneration
	})
	if !committed {
		// A concurrent projection advanced the repeater between the read and
		// the commit. Surface it as a failure so this sample is retried and
		// ordered by sequence instead of silently overwriting a newer sample.
		return fmt.Errorf("repeater %s projection changed concurrently for sample %d", sample.AssetID, sample.Sequence)
	}
	return nil
}

func (p *HealthProjector) Reset(repeaterID string, generation uint64) error {
	p.mu.RLock()
	repeater := p.repeaters[repeaterID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	if active, ok := p.incidents.Active(repeaterID); ok && generation < active {
		return nil
	}
	repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	if p.incidents != nil {
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
		_ = p.incidents.LastResolution(repeaterID)
	}
	return nil
}
