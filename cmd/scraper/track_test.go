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

	out, stats, _ := updateInFlightState(s, vehicles, now)

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

	out, stats, _ := updateInFlightState(s, vehicles, later)

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
	// LastSeenTS tracks the observation time (now), not the GPS fix time.
	if !trip.LastSeenTS.Equal(later) {
		t.Fatalf("LastSeenTS = %v, want %v (= now)", trip.LastSeenTS, later)
	}
	if len(trip.Probes) != 2 {
		t.Fatalf("Probes len = %d, want 2", len(trip.Probes))
	}
}

// Regression: a parked bus reports stale GPS (vs.TS doesn't refresh).
// LastSeenTS must track when WE observed the vehicle (now), not the
// stale GPS time, otherwise stale-prune fires on every cycle.
func TestUpdateInFlightStateLastSeenTSUsesNowNotGPSFixTime(t *testing.T) {
	gpsTS := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	observedNow := gpsTS.Add(25 * time.Minute) // 25 min later — would trip stale-prune at 20 min

	s := stateFile{
		InFlight: []inFlightTrip{{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			FirstSeenTS: gpsTS,
			LastSeenTS:  gpsTS,
			Probes:      []probe{{TS: gpsTS}},
		}},
	}
	vs := mkSnapshot("V1", "T1", "R1", gpsTS) // same stale GPS time
	out, _, _ := updateInFlightState(s, []vehicleSnapshot{vs}, observedNow)

	if len(out.InFlight) != 1 {
		t.Fatalf("len(InFlight) = %d, want 1", len(out.InFlight))
	}
	if !out.InFlight[0].LastSeenTS.Equal(observedNow) {
		t.Fatalf("LastSeenTS = %v, want %v (= now, not GPS time %v)",
			out.InFlight[0].LastSeenTS, observedNow, gpsTS)
	}
}

// AC Transit's GTFS-RT vehicle.timestamp is the GPS fix time, not the
// time of feed publication. When GPS hasn't refreshed between our minutely
// fetches, we get an identical probe. Without dedup we'd append duplicates,
// breaking dist-along-route progression and stop-arrival detection.
func TestUpdateInFlightStateDuplicateTimestampNotAppended(t *testing.T) {
	ts := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	s := stateFile{
		InFlight: []inFlightTrip{{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			FirstSeenTS: ts,
			LastSeenTS:  ts,
			Probes:      []probe{{TS: ts, Lat: 37.8, Lon: -122.0}},
		}},
	}
	// Same vehicle, same trip, SAME TS as the existing probe (caller may run
	// at any wall-clock time; only vs.TS controls dedup).
	vs := mkSnapshot("V1", "T1", "R1", ts)
	out, stats, _ := updateInFlightState(s, []vehicleSnapshot{vs}, ts.Add(time.Minute))

	if stats.ProbesAppended != 0 {
		t.Fatalf("ProbesAppended = %d, want 0 (duplicate TS should be skipped)", stats.ProbesAppended)
	}
	if len(out.InFlight) != 1 {
		t.Fatalf("len(InFlight) = %d, want 1", len(out.InFlight))
	}
	if len(out.InFlight[0].Probes) != 1 {
		t.Fatalf("Probes len = %d, want 1 (no duplicate append)", len(out.InFlight[0].Probes))
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

	out, stats, preempted := updateInFlightState(s, vehicles, later)

	if stats.NewTripsStarted != 1 {
		t.Fatalf("NewTripsStarted = %d, want 1", stats.NewTripsStarted)
	}
	if len(preempted) != 1 {
		t.Fatalf("preempted len = %d, want 1 (the old T1 trip)", len(preempted))
	}
	if preempted[0].TripID != "T1" {
		t.Fatalf("preempted[0].TripID = %q, want T1 (the trip that was replaced)", preempted[0].TripID)
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

func TestDetectCompletedTrips(t *testing.T) {
	cache := fixtureRouteCache() // R1/T1 with stop_seq 1, 2, 3 (last is 3)
	t0 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	t.Run("trip with arrival recorded for last stop is completed", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{
			{VehicleID: "V1", RouteID: "R1", TripID: "T1",
				StopArrivals: map[int]time.Time{1: t0, 2: t0.Add(time.Minute), 3: t0.Add(2 * time.Minute)}},
			{VehicleID: "V2", RouteID: "R1", TripID: "T1",
				StopArrivals: map[int]time.Time{1: t0}},
		}}
		completed := detectCompletedTrips(&s, cache)
		if len(completed) != 1 || completed[0].VehicleID != "V1" {
			t.Fatalf("completed = %+v, want one V1", completed)
		}
		if len(s.InFlight) != 1 || s.InFlight[0].VehicleID != "V2" {
			t.Fatalf("kept = %+v, want V2", s.InFlight)
		}
	})

	t.Run("trip with no last-stop arrival stays in flight", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{
			{VehicleID: "V1", RouteID: "R1", TripID: "T1",
				StopArrivals: map[int]time.Time{1: t0}},
		}}
		completed := detectCompletedTrips(&s, cache)
		if len(completed) != 0 {
			t.Fatalf("got %d completed, want 0", len(completed))
		}
		if len(s.InFlight) != 1 {
			t.Fatalf("InFlight len = %d, want 1", len(s.InFlight))
		}
	})

	t.Run("trip with unknown route is preserved (not finalized)", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{
			{VehicleID: "V1", RouteID: "UNKNOWN", TripID: "T1",
				StopArrivals: map[int]time.Time{99: t0}},
		}}
		completed := detectCompletedTrips(&s, cache)
		if len(completed) != 0 {
			t.Fatalf("completed = %+v, want 0 (unknown route shouldn't trigger completion)", completed)
		}
		if len(s.InFlight) != 1 {
			t.Fatalf("InFlight len = %d, want 1", len(s.InFlight))
		}
	})

	t.Run("nil cache is a no-op", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{
			{VehicleID: "V1", RouteID: "R1", TripID: "T1",
				StopArrivals: map[int]time.Time{3: t0}},
		}}
		completed := detectCompletedTrips(&s, nil)
		if completed != nil {
			t.Fatalf("got %+v, want nil", completed)
		}
		if len(s.InFlight) != 1 {
			t.Fatalf("InFlight len = %d, want 1", len(s.InFlight))
		}
	})
}

func TestPruneStaleTrips(t *testing.T) {
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-staleThreshold)
	staleTS := now.Add(-25 * time.Minute)
	freshTS := now.Add(-5 * time.Minute)

	t.Run("removes stale, keeps fresh, returns the stale ones", func(t *testing.T) {
		s := &stateFile{
			InFlight: []inFlightTrip{
				{VehicleID: "V_stale", LastSeenTS: staleTS},
				{VehicleID: "V_fresh", LastSeenTS: freshTS},
			},
		}
		stale := pruneStaleTrips(s, cutoff)
		if len(stale) != 1 || stale[0].VehicleID != "V_stale" {
			t.Fatalf("returned stale trips = %+v, want one V_stale", stale)
		}
		if len(s.InFlight) != 1 || s.InFlight[0].VehicleID != "V_fresh" {
			t.Fatalf("kept InFlight = %+v, want one V_fresh", s.InFlight)
		}
	})

	t.Run("empty state is a no-op", func(t *testing.T) {
		s := &stateFile{}
		if got := pruneStaleTrips(s, cutoff); got != nil {
			t.Fatalf("got %+v, want nil", got)
		}
	})

	t.Run("all-fresh keeps everything", func(t *testing.T) {
		s := &stateFile{InFlight: []inFlightTrip{{VehicleID: "V1", LastSeenTS: freshTS}}}
		if got := pruneStaleTrips(s, cutoff); len(got) != 0 {
			t.Fatalf("got %d stale, want 0", len(got))
		}
		if len(s.InFlight) != 1 {
			t.Fatalf("kept %d, want 1", len(s.InFlight))
		}
	})
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// fixtureRouteCache returns a minimal *gtfsCache containing one route
// (R1) with one trip (T1) on a straight east-west shape (S1) and three
// stops at known dist_along_route values: 0, 200, 500 meters.
func fixtureRouteCache() *gtfsCache {
	shape := [][3]float64{
		{0, 0.0000, 0},
		{0, 0.0010, 111.32},
		{0, 0.0020, 222.64},
		{0, 0.0030, 333.96},
		{0, 0.0050, 556.60},
	}
	trip := gtfsTrip{
		TripID:  "T1",
		ShapeID: "S1",
		StopTimes: []gtfsStopTime{
			{StopSequence: 1, StopID: "A", DistAlongRoute: 0},
			{StopSequence: 2, StopID: "B", DistAlongRoute: 200},
			{StopSequence: 3, StopID: "C", DistAlongRoute: 500},
		},
	}
	return &gtfsCache{
		Routes: map[string]*processedGTFSRoute{
			"R1": {
				RouteID: "R1",
				Shapes:  map[string][][3]float64{"S1": shape},
				Trips:   map[string]gtfsTrip{"T1": trip},
				Stops: map[string]gtfsStop{
					"A": {StopID: "A", Lat: 0, Lon: 0.0000},
					"B": {StopID: "B", Lat: 0, Lon: 0.0018},
					"C": {StopID: "C", Lat: 0, Lon: 0.0045},
				},
			},
		},
		FeedHash:   "fixture",
		Generation: 1,
	}
}

func TestProjectInFlightProbes(t *testing.T) {
	cache := fixtureRouteCache()
	now := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	t.Run("known route fills dist_along_route_m and nearest_stop_seq", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{{TS: now, Lat: 0, Lon: 0.0010}},
		}}}
		var stats trackStats
		projectInFlightProbes(&s, cache, &stats)

		p := s.InFlight[0].Probes[0]
		if p.DistAlongRouteM < 100 || p.DistAlongRouteM > 130 {
			t.Fatalf("DistAlongRouteM = %.2f, want ~111.32", p.DistAlongRouteM)
		}
		// At ~111 m, distances are: |111-0|=111 vs |111-200|=89. B (seq 2) is closer.
		if p.NearestStopSeq != 2 {
			t.Fatalf("NearestStopSeq = %d, want 2", p.NearestStopSeq)
		}
	})

	t.Run("unknown route increments TripsMissingShape and leaves probes untouched", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "UNKNOWN", TripID: "T1",
			Probes: []probe{{TS: now, Lat: 0, Lon: 0.0010}},
		}}}
		var stats trackStats
		projectInFlightProbes(&s, cache, &stats)

		if stats.TripsMissingShape != 1 {
			t.Fatalf("TripsMissingShape = %d, want 1", stats.TripsMissingShape)
		}
		if s.InFlight[0].Probes[0].DistAlongRouteM != 0 {
			t.Fatalf("DistAlongRouteM = %v, want 0 (unchanged)", s.InFlight[0].Probes[0].DistAlongRouteM)
		}
	})

	t.Run("probe with prior projection is not overwritten", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{{TS: now, Lat: 0, Lon: 0.0010, DistAlongRouteM: 999, NearestStopSeq: 7}},
		}}}
		var stats trackStats
		projectInFlightProbes(&s, cache, &stats)

		p := s.InFlight[0].Probes[0]
		if p.DistAlongRouteM != 999 || p.NearestStopSeq != 7 {
			t.Fatalf("got DistAlong=%.2f Seq=%d, want unchanged 999/7", p.DistAlongRouteM, p.NearestStopSeq)
		}
	})
}

func TestDetectStopArrivals(t *testing.T) {
	cache := fixtureRouteCache()
	t0 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	t.Run("probe before first stop records no arrival", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{{TS: t0, DistAlongRouteM: -10}},
		}}}
		var stats trackStats
		detectStopArrivals(&s, cache, &stats)
		if len(s.InFlight[0].StopArrivals) != 0 {
			t.Fatalf("got %d arrivals, want 0", len(s.InFlight[0].StopArrivals))
		}
	})

	t.Run("single probe past stops records nothing — never witnessed crossing", func(t *testing.T) {
		// Bus appears to be at dist=600 (past all 3 stops). With only one
		// probe, we have no evidence of WHEN it crossed each stop, so we
		// must record nothing.
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes:    []probe{{TS: t0, DistAlongRouteM: 600}},
		}}}
		var stats trackStats
		detectStopArrivals(&s, cache, &stats)
		if len(s.InFlight[0].StopArrivals) != 0 {
			t.Fatalf("got %d arrivals from a single probe, want 0", len(s.InFlight[0].StopArrivals))
		}
		if stats.StopArrivalsDetected != 0 {
			t.Fatalf("StopArrivalsDetected = %d, want 0", stats.StopArrivalsDetected)
		}
	})

	t.Run("two probes bracket stop B; arrival is interpolated", func(t *testing.T) {
		// p1 at dist=100 t=t0, p2 at dist=300 t=t0+60s. Stop B at 200.
		// frac = (200-100)/(300-100) = 0.5 → arrival = t0 + 30s.
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{
				{TS: t0, DistAlongRouteM: 100},
				{TS: t0.Add(60 * time.Second), DistAlongRouteM: 300},
			},
		}}}
		var stats trackStats
		detectStopArrivals(&s, cache, &stats)
		got := s.InFlight[0].StopArrivals[2]
		want := t0.Add(30 * time.Second)
		if !got.Equal(want) {
			t.Fatalf("StopArrivals[2] = %v, want %v (interpolated midway)", got, want)
		}
	})

	t.Run("already-recorded arrival is not overwritten on later probe", func(t *testing.T) {
		first := t0
		later := t0.Add(5 * time.Minute)
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID:    "V1", RouteID: "R1", TripID: "T1",
			StopArrivals: map[int]time.Time{1: first},
			Probes:       []probe{{TS: later, DistAlongRouteM: 50}},
		}}}
		var stats trackStats
		detectStopArrivals(&s, cache, &stats)
		if got := s.InFlight[0].StopArrivals[1]; !got.Equal(first) {
			t.Fatalf("StopArrivals[1] = %v, want %v (preserved)", got, first)
		}
		if stats.StopArrivalsDetected != 0 {
			t.Fatalf("StopArrivalsDetected = %d, want 0", stats.StopArrivalsDetected)
		}
	})

	t.Run("two probes that bracket multiple stops records all of them", func(t *testing.T) {
		// p1 at dist=-50 t=t0, p2 at dist=600 t=t0+60s. Crosses A (0), B (200), C (500).
		// All three should be recorded with interpolated timestamps.
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{
				{TS: t0, DistAlongRouteM: -50},
				{TS: t0.Add(60 * time.Second), DistAlongRouteM: 600},
			},
		}}}
		var stats trackStats
		detectStopArrivals(&s, cache, &stats)
		if len(s.InFlight[0].StopArrivals) != 3 {
			t.Fatalf("got %d arrivals, want 3 (A, B, C all crossed between probes)", len(s.InFlight[0].StopArrivals))
		}
		// Stop A at dist 0: frac = (0 - (-50)) / (600 - (-50)) ≈ 0.0769
		// → arrival ≈ t0 + 4.6s
		gotA := s.InFlight[0].StopArrivals[1]
		fracNanos := float64(60*time.Second) * 50 / 650
		wantA := t0.Add(time.Duration(fracNanos))
		if absDur(gotA.Sub(wantA)) > 100*time.Millisecond {
			t.Fatalf("StopArrivals[1] = %v, want ~%v", gotA, wantA)
		}
	})

	t.Run("trip with unknown route in cache is skipped without panicking", func(t *testing.T) {
		s := stateFile{InFlight: []inFlightTrip{{
			VehicleID: "V1", RouteID: "UNKNOWN", TripID: "T1",
			Probes:    []probe{{TS: t0, DistAlongRouteM: 1000}},
		}}}
		var stats trackStats
		detectStopArrivals(&s, cache, &stats)
		if len(s.InFlight[0].StopArrivals) != 0 {
			t.Fatalf("got %d arrivals on unknown route, want 0", len(s.InFlight[0].StopArrivals))
		}
	})
}

// fixtureRouteCache has stops at dist 0, 200, 500. The trailing-stop
// fallback must recover the last stop when the bus's max observed dist
// is just shy of 500, and must NOT fire when the bus stalled mid-route.
func TestApplyTrailingStopFallback(t *testing.T) {
	cache := fixtureRouteCache()
	t0 := time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)

	t.Run("max probe within tolerance of last stop attributes last stop", func(t *testing.T) {
		// Stops at 200 and 500 already detected; final probe is at 480
		// (20 m shy of last stop). With 150 m tolerance, last stop
		// gets attributed using the final probe's TS.
		final := t0.Add(10 * time.Minute)
		trip := &inFlightTrip{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{
				{TS: t0, DistAlongRouteM: 0},
				{TS: t0.Add(2 * time.Minute), DistAlongRouteM: 200},
				{TS: final, DistAlongRouteM: 480},
			},
			StopArrivals: map[int]time.Time{1: t0, 2: t0.Add(2 * time.Minute)},
		}
		added := applyTrailingStopFallback(trip, cache, 150)
		if added != 1 {
			t.Fatalf("added = %d, want 1", added)
		}
		got, ok := trip.StopArrivals[3]
		if !ok || !got.Equal(final) {
			t.Fatalf("StopArrivals[3] = %v ok=%v, want %v", got, ok, final)
		}
	})

	t.Run("max probe far from last stop does not attribute", func(t *testing.T) {
		// Final probe at 200; last stop at 500 (300 m gap > 150 tolerance).
		trip := &inFlightTrip{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{
				{TS: t0, DistAlongRouteM: 0},
				{TS: t0.Add(2 * time.Minute), DistAlongRouteM: 200},
			},
			StopArrivals: map[int]time.Time{1: t0, 2: t0.Add(2 * time.Minute)},
		}
		added := applyTrailingStopFallback(trip, cache, 150)
		if added != 0 {
			t.Fatalf("added = %d, want 0 (gap too large)", added)
		}
		if _, ok := trip.StopArrivals[3]; ok {
			t.Fatalf("StopArrivals[3] was set; should not have been")
		}
	})

	t.Run("walks backward attributing multiple trailing stops within tolerance", func(t *testing.T) {
		// fixture stops: 0, 200, 500. Place bus's max at 380 — within
		// tolerance of stop 3 (500 - 380 = 120 < 150) AND stop 2 (200
		// is behind max so already-attributed already by precondition).
		// Make stop 2 NOT pre-attributed: walk backward should set both.
		final := t0.Add(8 * time.Minute)
		trip := &inFlightTrip{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{
				{TS: t0, DistAlongRouteM: 0},
				{TS: final, DistAlongRouteM: 380},
			},
			StopArrivals: map[int]time.Time{1: t0},
		}
		added := applyTrailingStopFallback(trip, cache, 150)
		if added != 2 {
			t.Fatalf("added = %d, want 2 (both 2 and 3 attributed)", added)
		}
		if got := trip.StopArrivals[2]; !got.Equal(final) {
			t.Fatalf("StopArrivals[2] = %v, want %v", got, final)
		}
		if got := trip.StopArrivals[3]; !got.Equal(final) {
			t.Fatalf("StopArrivals[3] = %v, want %v", got, final)
		}
	})

	t.Run("no-op when last stop already attributed", func(t *testing.T) {
		ts3 := t0.Add(10 * time.Minute)
		trip := &inFlightTrip{
			VehicleID: "V1", RouteID: "R1", TripID: "T1",
			Probes: []probe{
				{TS: t0, DistAlongRouteM: 0},
				{TS: ts3, DistAlongRouteM: 510},
			},
			StopArrivals: map[int]time.Time{3: ts3},
		}
		added := applyTrailingStopFallback(trip, cache, 150)
		if added != 0 {
			t.Fatalf("added = %d, want 0 (already attributed)", added)
		}
	})

	t.Run("nil cache no-op", func(t *testing.T) {
		trip := &inFlightTrip{Probes: []probe{{TS: t0, DistAlongRouteM: 480}}}
		if added := applyTrailingStopFallback(trip, nil, 150); added != 0 {
			t.Fatalf("added = %d, want 0", added)
		}
	})

	t.Run("empty probes no-op", func(t *testing.T) {
		trip := &inFlightTrip{VehicleID: "V1", RouteID: "R1", TripID: "T1"}
		if added := applyTrailingStopFallback(trip, cache, 150); added != 0 {
			t.Fatalf("added = %d, want 0", added)
		}
	})

	t.Run("unknown route no-op", func(t *testing.T) {
		trip := &inFlightTrip{
			VehicleID: "V1", RouteID: "UNKNOWN", TripID: "T1",
			Probes: []probe{{TS: t0, DistAlongRouteM: 480}},
		}
		if added := applyTrailingStopFallback(trip, cache, 150); added != 0 {
			t.Fatalf("added = %d, want 0", added)
		}
	})
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

	out, _, _ := updateInFlightState(s, vehicles, newTS)

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
