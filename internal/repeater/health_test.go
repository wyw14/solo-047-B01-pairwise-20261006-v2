package repeater

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type fakeIncidentSink struct {
	mu           sync.Mutex
	opened       int
	openFailures int
	resolved     int
	active       map[string]uint64
	resolutions  map[string]uint64
}

func newFakeIncidentSink(openFailures int) *fakeIncidentSink {
	return &fakeIncidentSink{openFailures: openFailures, active: map[string]uint64{}, resolutions: map[string]uint64{}}
}

func (f *fakeIncidentSink) OpenRepeaterIncident(repeaterID string, generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.openFailures > 0 {
		f.openFailures--
		return errors.New("incident store unavailable")
	}
	f.opened++
	f.active[repeaterID] = generation
	return nil
}

func (f *fakeIncidentSink) ResolveRepeaterIncident(repeaterID string, generation uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolved++
	delete(f.active, repeaterID)
	f.resolutions[repeaterID] = generation
	return nil
}

func (f *fakeIncidentSink) Active(repeaterID string) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	generation, ok := f.active[repeaterID]
	return generation, ok
}

func (f *fakeIncidentSink) LastResolution(repeaterID string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resolutions[repeaterID]
}

func temperatureSample(sequence, generation uint64, value float64) telemetry.Sample {
	return telemetry.Sample{
		ID: "s1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: value, Unit: "C",
		Sequence: sequence, Generation: generation,
		ObservedAt: time.Date(2026, 10, 5, 3, 0, 0, 0, time.UTC),
	}
}

func TestProjectorIncidentFailureLeavesStateUntouchedAndRetryCommitsOnce(t *testing.T) {
	asset, err := New("R07", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	incidents := newFakeIncidentSink(1) // first open attempt fails
	projector := NewHealthProjector(incidents, 85)
	projector.Register(asset)

	sample := temperatureSample(1, 7, 90)
	if err := projector.ApplySample(context.Background(), sample); err == nil {
		t.Fatal("expected incident-open failure")
	}

	snapshot := asset.Snapshot()
	if snapshot.Health != HealthNormal || snapshot.LastSequence != 0 || snapshot.TripGeneration != 0 {
		t.Fatalf("failed projection left dirty state: %+v", snapshot)
	}
	if incidents.opened != 0 {
		t.Fatalf("incident must not count a failed open")
	}

	// Retry the same sample: now it commits, and the incident is effectively
	// opened exactly once.
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatalf("retry projection: %v", err)
	}
	snapshot = asset.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.LastSequence != 1 || snapshot.TripGeneration != 7 {
		t.Fatalf("retry did not commit projection: %+v", snapshot)
	}
	if incidents.opened != 1 {
		t.Fatalf("incident opened %d times, want exactly 1 effective open", incidents.opened)
	}

	// Re-delivering the same (or older) sample after commit is a no-op.
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatalf("idempotent re-apply: %v", err)
	}
	if incidents.opened != 1 || asset.Snapshot().LastSequence != 1 {
		t.Fatalf("re-delivery changed projection: %+v opens=%d", asset.Snapshot(), incidents.opened)
	}
}

func TestProjectorIgnoresIrrelevantSamples(t *testing.T) {
	asset, err := New("R07", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	incidents := newFakeIncidentSink(0)
	projector := NewHealthProjector(incidents, 85)
	projector.Register(asset)

	current := telemetry.Sample{
		ID: "s2", AssetID: "R07", Kind: telemetry.KindCurrent, Value: 900,
		Sequence: 1, ObservedAt: time.Now().UTC(),
	}
	if err := projector.ApplySample(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	if snapshot := asset.Snapshot(); snapshot.Health != HealthNormal || snapshot.LastSequence != 0 {
		t.Fatalf("non-temperature sample altered projection: %+v", snapshot)
	}
}

func TestProjectorNormalTemperatureUpdatesSequenceWithoutIncident(t *testing.T) {
	asset, err := New("R07", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	incidents := newFakeIncidentSink(0)
	projector := NewHealthProjector(incidents, 85)
	projector.Register(asset)

	if err := projector.ApplySample(context.Background(), temperatureSample(1, 1, 40)); err != nil {
		t.Fatal(err)
	}
	snapshot := asset.Snapshot()
	if snapshot.Health != HealthNormal || snapshot.LastSequence != 1 || snapshot.TemperatureC != 40 {
		t.Fatalf("normal sample projection wrong: %+v", snapshot)
	}
	if incidents.opened != 0 {
		t.Fatal("normal temperature opened an incident")
	}
}
