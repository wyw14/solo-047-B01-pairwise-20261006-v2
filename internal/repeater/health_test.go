package repeater

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type fakeIncidents struct {
	opens    []uint64
	failNext bool
}

func (f *fakeIncidents) OpenRepeaterIncident(_ string, generation uint64) error {
	if f.failNext {
		f.failNext = false
		return errors.New("incident store unavailable")
	}
	f.opens = append(f.opens, generation)
	return nil
}

func (f *fakeIncidents) ResolveRepeaterIncident(string, uint64) error { return nil }
func (f *fakeIncidents) Active(string) (uint64, bool)                { return 0, false }
func (f *fakeIncidents) LastResolution(string) uint64                { return 0 }

func temperatureSample(id string, sequence, generation uint64, value float64) telemetry.Sample {
	return telemetry.Sample{
		ID: id, AssetID: "R07", Kind: telemetry.KindTemperature, Value: value, Unit: "C",
		Sequence: sequence, Generation: generation, ObservedAt: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC),
	}
}

func TestTripProjectionRetriesAndOpensIncidentOnce(t *testing.T) {
	incidents := &fakeIncidents{failNext: true}
	projector := NewHealthProjector(incidents, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	sample := temperatureSample("sample-1", 1, 7, 90)

	// First attempt fails while opening the incident: local state must not
	// advance, otherwise the retry's sequence guard would skip the open.
	if err := projector.ApplySample(context.Background(), sample); err == nil {
		t.Fatal("expected first projection to fail")
	}
	if snapshot := asset.Snapshot(); snapshot.LastSequence != 0 || snapshot.Health != HealthNormal {
		t.Fatalf("state committed on failed projection: %+v", snapshot)
	}

	// Redelivering the same sample must still open the incident.
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	snapshot := asset.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.LastSequence != 1 || snapshot.TripGeneration != 7 {
		t.Fatalf("unexpected snapshot after retry: %+v", snapshot)
	}
	if len(incidents.opens) != 1 || incidents.opens[0] != 7 {
		t.Fatalf("incident opened %v, want exactly once at generation 7", incidents.opens)
	}

	// A third delivery of the same sample ID is a no-op: no duplicate open and
	// no state regression.
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if len(incidents.opens) != 1 {
		t.Fatalf("replay opened incident again: %v", incidents.opens)
	}
	if asset.Snapshot().LastSequence != 1 {
		t.Fatalf("replay advanced state: %+v", asset.Snapshot())
	}
}

func TestNonTrippingSampleProjectsOnce(t *testing.T) {
	incidents := &fakeIncidents{}
	projector := NewHealthProjector(incidents, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	sample := temperatureSample("sample-2", 5, 1, 40)
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	if snapshot := asset.Snapshot(); snapshot.Health != HealthNormal || snapshot.LastSequence != 5 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if len(incidents.opens) != 0 {
		t.Fatalf("incident opened for healthy sample: %v", incidents.opens)
	}
}
