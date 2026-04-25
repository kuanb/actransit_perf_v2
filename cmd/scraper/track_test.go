package main

import (
	"testing"
	"time"
)

func mkSnapshot(vid, tripID, routeID string, ts time.Time) vehicleSnapshot {
	return vehicleSnapshot{
		VehicleID: vid,
		RouteID:   routeID,
		TripID:    tripID,
		StartDate: "20260424",
		TS:        ts,
		Lat:       37.8,
		Lon:       -122.0,
		Bearing:   0,
		SpeedMps:  0,
	}
}

func TestUpdateInFlightStateNewVehicle(t *testing.T) {
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	s := stateFile{}
	vehicles := []vehicleSnapshot{mkSnapshot("V1", "T1", "R1", now)}

	out, stats := updateInFlightState(s, vehicles, now)

	if stats.NewTripsStarted != 1 {
		t.Fatalf("NewTripsStarted = %d, want 1", stats.NewTripsStarted)
	}
	if stats.InFlight != 1 {
		t.Fatalf("InFlight = %d, want 1", stats.InFlight)
	}
	if len(out.InFlight) != 1 {
		t.Fatalf("len(InFlight) = %d, want 1", len(out.InFlight))
	}
	if out.InFlight[0].VehicleID != "V1" || out.InFlight[0].TripID != "T1" {
		t.Fatalf("got %+v, want V1/T1", out.InFlight[0])
	}
	if len(out.InFlight[0].Probes) != 1 {
		t.Fatalf("Probes len = %d, want 1", len(out.InFlight[0].Probes))
	}
}

func TestUpdateInFlightStateContinuingTrip(t *testing.T) {
	earlier := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	later := earlier.Add(30 * time.Second)

	s := stateFile{
		InFlight: []inFlightTrip{{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			ServiceDate: "20260424",
			FirstSeenTS: earlier,
			LastSeenTS:  earlier,
			Probes:      []probe{{TS: earlier, Lat: 37.8, Lon: -122.0}},
		}},
	}
	vehicles := []vehicleSnapshot{mkSnapshot("V1", "T1", "R1", later)}

	out, stats := updateInFlightState(s, vehicles, later)

	if stats.ProbesAppended != 1 {
		t.Fatalf("ProbesAppended = %d, want 1", stats.ProbesAppended)
	}
	if stats.NewTripsStarted != 0 || stats.TripsExpired != 0 {
		t.Fatalf("unexpected lifecycle stats: %+v", stats)
	}
	if len(out.InFlight) != 1 {
		t.Fatalf("len(InFlight) = %d, want 1", len(out.InFlight))
	}
	trip := out.InFlight[0]
	if !trip.LastSeenTS.Equal(later) {
		t.Fatalf("LastSeenTS = %v, want %v", trip.LastSeenTS, later)
	}
	if len(trip.Probes) != 2 {
		t.Fatalf("Probes len = %d, want 2", len(trip.Probes))
	}
}

func TestUpdateInFlightStateTripChange(t *testing.T) {
	earlier := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	later := earlier.Add(30 * time.Second)

	s := stateFile{
		InFlight: []inFlightTrip{{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			FirstSeenTS: earlier,
			LastSeenTS:  earlier,
			Probes:      []probe{{TS: earlier}},
		}},
	}
	vehicles := []vehicleSnapshot{mkSnapshot("V1", "T2", "R1", later)}

	out, stats := updateInFlightState(s, vehicles, later)

	if stats.NewTripsStarted != 1 {
		t.Fatalf("NewTripsStarted = %d, want 1", stats.NewTripsStarted)
	}
	if stats.TripsExpired != 1 {
		t.Fatalf("TripsExpired = %d, want 1", stats.TripsExpired)
	}
	if len(out.InFlight) != 1 {
		t.Fatalf("len(InFlight) = %d, want 1", len(out.InFlight))
	}
	if out.InFlight[0].TripID != "T2" {
		t.Fatalf("TripID = %q, want T2", out.InFlight[0].TripID)
	}
	if len(out.InFlight[0].Probes) != 1 {
		t.Fatalf("Probes len = %d, want 1 (fresh trip)", len(out.InFlight[0].Probes))
	}
}

func TestUpdateInFlightStateStalePruning(t *testing.T) {
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-25 * time.Minute) // > staleThreshold (20 min)

	s := stateFile{
		InFlight: []inFlightTrip{{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			FirstSeenTS: stale,
			LastSeenTS:  stale,
			Probes:      []probe{{TS: stale}},
		}},
	}
	out, stats := updateInFlightState(s, nil, now)

	if stats.TripsExpired != 1 {
		t.Fatalf("TripsExpired = %d, want 1", stats.TripsExpired)
	}
	if stats.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", stats.InFlight)
	}
	if len(out.InFlight) != 0 {
		t.Fatalf("len(InFlight) = %d, want 0", len(out.InFlight))
	}
}

func TestUpdateInFlightStateProbeCap(t *testing.T) {
	base := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	probes := make([]probe, maxProbesPerTrip)
	for i := range probes {
		probes[i] = probe{TS: base.Add(time.Duration(i) * time.Second)}
	}
	priorOldest := probes[0].TS

	s := stateFile{
		InFlight: []inFlightTrip{{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			FirstSeenTS: base,
			LastSeenTS:  probes[len(probes)-1].TS,
			Probes:      probes,
		}},
	}
	newTS := base.Add(time.Duration(maxProbesPerTrip) * time.Second)
	vehicles := []vehicleSnapshot{mkSnapshot("V1", "T1", "R1", newTS)}

	out, _ := updateInFlightState(s, vehicles, newTS)

	if len(out.InFlight) != 1 {
		t.Fatalf("len(InFlight) = %d, want 1", len(out.InFlight))
	}
	trip := out.InFlight[0]
	if len(trip.Probes) != maxProbesPerTrip {
		t.Fatalf("Probes len = %d, want %d (capped)",
			len(trip.Probes), maxProbesPerTrip)
	}
	if trip.Probes[0].TS.Equal(priorOldest) {
		t.Fatalf("oldest probe was not dropped: Probes[0].TS = %v (== prior oldest)",
			trip.Probes[0].TS)
	}
	if !trip.Probes[len(trip.Probes)-1].TS.Equal(newTS) {
		t.Fatalf("newest probe TS = %v, want %v",
			trip.Probes[len(trip.Probes)-1].TS, newTS)
	}
}
