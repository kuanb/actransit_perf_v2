package main

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func TestParseServiceDate(t *testing.T) {
	cases := []struct {
		in   string
		want civil.Date
	}{
		{"20260424", civil.Date{Year: 2026, Month: 4, Day: 24}},
		{"20251231", civil.Date{Year: 2025, Month: 12, Day: 31}},
		{"", civil.Date{}},
		{"2026042", civil.Date{}},
		{"abcdefgh", civil.Date{}},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got := parseServiceDate(c.in)
			if got != c.want {
				t.Fatalf("parseServiceDate(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

func TestParseScheduledArrival(t *testing.T) {
	sd := civil.Date{Year: 2026, Month: 4, Day: 24}

	t.Run("normal time", func(t *testing.T) {
		got := parseScheduledArrival(sd, "08:30:00")
		// 08:30:00 PT on 2026-04-24 → 15:30:00 UTC (PDT is UTC-7)
		want := time.Date(2026, 4, 24, 15, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("post-midnight time (25:30:00 → next-day 01:30 PT)", func(t *testing.T) {
		got := parseScheduledArrival(sd, "25:30:00")
		// 2026-04-25 01:30:00 PT → 08:30:00 UTC
		want := time.Date(2026, 4, 25, 8, 30, 0, 0, time.UTC)
		if !got.Equal(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("empty string returns zero", func(t *testing.T) {
		if got := parseScheduledArrival(sd, ""); !got.IsZero() {
			t.Fatalf("got %v, want zero", got)
		}
	})

	t.Run("malformed string returns zero", func(t *testing.T) {
		if got := parseScheduledArrival(sd, "not-a-time"); !got.IsZero() {
			t.Fatalf("got %v, want zero", got)
		}
	})
}

func TestTripToRows(t *testing.T) {
	cache := fixtureRouteCache()
	// Fixture cache uses lat,lon-only stops without arrival_time strings.
	// Inject scheduled arrivals so we can exercise delay computation.
	r := cache.Routes["R1"]
	trip := r.Trips["T1"]
	for i := range trip.StopTimes {
		trip.StopTimes[i].ArrivalTime = []string{"08:00:00", "08:01:00", "08:03:00"}[i]
	}
	r.Trips["T1"] = trip

	t0 := time.Date(2026, 4, 24, 15, 0, 0, 0, time.UTC) // 08:00 PDT
	ingestedAt := t0.Add(time.Hour)

	t.Run("happy path: arrivals + GTFS produce observations + probes", func(t *testing.T) {
		trip := inFlightTrip{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			ServiceDate: "20260424",
			Probes: []probe{
				{TS: t0, Lat: 0, Lon: 0, DistAlongRouteM: 0, NearestStopSeq: 1, BearingDeg: 0, ReportedSpeedMps: 0},
				{TS: t0.Add(2 * time.Minute), Lat: 0, Lon: 0.005, DistAlongRouteM: 550, NearestStopSeq: 3, BearingDeg: 90, ReportedSpeedMps: 4.5},
			},
			StopArrivals: map[int]time.Time{
				1: t0,
				2: t0.Add(70 * time.Second),
				3: t0.Add(150 * time.Second),
			},
		}
		obs, probes := tripToRows(trip, cache, ingestedAt, true)

		if len(obs) != 3 {
			t.Fatalf("len(obs) = %d, want 3", len(obs))
		}
		if len(probes) != 2 {
			t.Fatalf("len(probes) = %d, want 2", len(probes))
		}

		// First obs: stop 1 at scheduled 08:00 = t0 = actual t0 → delay 0.
		o1 := obs[0]
		if o1.StopSequence != 1 || o1.StopID != "A" {
			t.Fatalf("obs[0] stop = %d/%s, want 1/A", o1.StopSequence, o1.StopID)
		}
		if !o1.ScheduledArrival.Valid || !o1.ScheduledArrival.Timestamp.Equal(t0) {
			t.Fatalf("obs[0] scheduled = %v, want %v", o1.ScheduledArrival, t0)
		}
		if !o1.ActualArrival.Valid || !o1.ActualArrival.Timestamp.Equal(t0) {
			t.Fatalf("obs[0] actual = %v, want %v", o1.ActualArrival, t0)
		}
		if !o1.DelaySeconds.Valid || o1.DelaySeconds.Int64 != 0 {
			t.Fatalf("obs[0] delay = %v, want 0", o1.DelaySeconds)
		}
		if !o1.IsStale {
			t.Fatalf("obs[0] is_stale = false, want true")
		}
		if o1.LegDistanceM.Valid {
			t.Fatalf("obs[0] leg_distance = %v, want NULL (first stop)", o1.LegDistanceM)
		}

		// Second obs: stop 2 at scheduled 08:01 = t0+60s, actual t0+70s → delay 10s.
		o2 := obs[1]
		if !o2.DelaySeconds.Valid || o2.DelaySeconds.Int64 != 10 {
			t.Fatalf("obs[1] delay = %v, want 10s", o2.DelaySeconds)
		}
		// Leg from stop 1 (dist 0, t0) to stop 2 (dist 200, t0+70s) → 200m / 70s ≈ 2.857 m/s.
		if !o2.LegDistanceM.Valid || o2.LegDistanceM.Float64 != 200 {
			t.Fatalf("obs[1] leg_distance = %v, want 200", o2.LegDistanceM)
		}
		if !o2.LegDurationS.Valid || o2.LegDurationS.Float64 != 70 {
			t.Fatalf("obs[1] leg_duration = %v, want 70", o2.LegDurationS)
		}

		// First probe: BearingDeg=0, SpeedMps=0, NearestStopSeq=1 → all
		// stored as zero or NULL per the row builder's "skip-if-zero" logic.
		p1 := probes[0]
		if p1.BearingDeg.Valid {
			t.Fatalf("probes[0] bearing = %v, want NULL (zero value)", p1.BearingDeg)
		}
		if !p1.NearestStopSeq.Valid || p1.NearestStopSeq.Int64 != 1 {
			t.Fatalf("probes[0] nearest_stop_seq = %v, want 1", p1.NearestStopSeq)
		}
		// Second probe has nonzero values so should be set.
		p2 := probes[1]
		if !p2.BearingDeg.Valid || p2.BearingDeg.Float64 != 90 {
			t.Fatalf("probes[1] bearing = %v, want 90", p2.BearingDeg)
		}
	})

	t.Run("trip with unknown route produces 0 observations but probe rows still emit", func(t *testing.T) {
		trip := inFlightTrip{
			VehicleID:   "V1",
			RouteID:     "UNKNOWN",
			TripID:      "T1",
			ServiceDate: "20260424",
			Probes:      []probe{{TS: t0, DistAlongRouteM: 100}},
		}
		obs, probes := tripToRows(trip, cache, ingestedAt, false)
		if len(obs) != 0 {
			t.Fatalf("len(obs) = %d, want 0 (no route in cache)", len(obs))
		}
		if len(probes) != 1 {
			t.Fatalf("len(probes) = %d, want 1", len(probes))
		}
		if probes[0].RouteID != "UNKNOWN" {
			t.Fatalf("probes[0] route = %q, want UNKNOWN", probes[0].RouteID)
		}
	})

	t.Run("trip with no actual arrivals leaves actual_arrival NULL but keeps scheduled", func(t *testing.T) {
		trip := inFlightTrip{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			ServiceDate: "20260424",
			Probes:      []probe{{TS: t0, DistAlongRouteM: -10}},
			// no StopArrivals map populated
		}
		obs, _ := tripToRows(trip, cache, ingestedAt, true)
		if len(obs) != 3 {
			t.Fatalf("len(obs) = %d, want 3", len(obs))
		}
		for i, o := range obs {
			if o.ActualArrival.Valid {
				t.Fatalf("obs[%d] actual = %v, want NULL", i, o.ActualArrival)
			}
			if !o.ScheduledArrival.Valid {
				t.Fatalf("obs[%d] scheduled missing", i)
			}
			if o.DelaySeconds.Valid {
				t.Fatalf("obs[%d] delay = %v, want NULL", i, o.DelaySeconds)
			}
		}
	})

	t.Run("malformed service_date short-circuits service_date and scheduled_arrival but rows still emit", func(t *testing.T) {
		trip := inFlightTrip{
			VehicleID:   "V1",
			RouteID:     "R1",
			TripID:      "T1",
			ServiceDate: "BADDATE",
			Probes:      []probe{{TS: t0, DistAlongRouteM: 100}},
			StopArrivals: map[int]time.Time{1: t0},
		}
		obs, probes := tripToRows(trip, cache, ingestedAt, true)
		if len(obs) == 0 || len(probes) == 0 {
			t.Fatalf("expected rows even with bad service_date; got obs=%d probes=%d", len(obs), len(probes))
		}
		if obs[0].ScheduledArrival.Valid {
			t.Fatalf("scheduled should be NULL on bad service_date")
		}
	})
}
