package repeater

import (
	"context"
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
	// applied records sample IDs that have already crossed the projection
	// boundary, so a retried sample never opens an incident or advances state
	// twice.
	applied   map[string]struct{}
	incidents IncidentSink
	tripAtC   float64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{
		repeaters: make(map[string]*Repeater), applied: make(map[string]struct{}),
		incidents: incidents, tripAtC: tripAtC,
	}
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = repeater
}

func (p *HealthProjector) markApplied(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applied[id] = struct{}{}
}

func (p *HealthProjector) alreadyApplied(id string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.applied[id]
	return ok
}

// ApplySample projects one temperature sample into repeater health. The
// projection is retriable and idempotent:
//
//   - A sample ID that already projected successfully is a no-op, so an
//     inbound retry can never open an incident (or advance the sequence)
//     twice.
//   - The incident side effect happens BEFORE the repeater snapshot is
//     mutated. If opening the incident fails, local state is left untouched
//     and the sample is not marked applied, so redelivery of the same sample
//     still passes the sequence guard and retries the open. Only once the
//     side effect is durable does the health state commit.
func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if p.alreadyApplied(sample.ID) {
		return nil
	}
	p.mu.RLock()
	repeater := p.repeaters[sample.AssetID]
	p.mu.RUnlock()
	if sample.Kind != telemetry.KindTemperature || repeater == nil {
		p.markApplied(sample.ID)
		return nil
	}
	before := repeater.Snapshot()
	if sample.Sequence <= before.LastSequence || sample.Generation < before.TripGeneration {
		p.markApplied(sample.ID)
		return nil
	}

	willTrip := before.Health != HealthTripped && sample.Value >= p.tripAtC
	if willTrip && p.incidents != nil {
		if err := p.incidents.OpenRepeaterIncident(before.ID, sample.Generation); err != nil {
			return err
		}
	}
	repeater.update(func(current *Snapshot) {
		current.LastSequence = sample.Sequence
		current.TemperatureC = sample.Value
		current.UpdatedAt = sample.ObservedAt.UTC()
		if sample.Value >= p.tripAtC {
			current.Health = HealthTripped
			current.TripGeneration = sample.Generation
		} else if current.Health != HealthTripped {
			current.Health = HealthNormal
		}
	})
	p.markApplied(sample.ID)
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
	// Resolve the incident before committing the local reset: a failed side
	// effect must not leave local health claiming normal while the incident is
	// still active.
	if p.incidents != nil {
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
		_ = p.incidents.LastResolution(repeaterID)
	}
	repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	return nil
}
