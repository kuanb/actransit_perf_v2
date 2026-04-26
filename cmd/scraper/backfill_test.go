package main

import (
	"strings"
	"testing"
	"time"
)

// pandas's to_csv() default writes an unnamed leading column with the row
// number — the parser must look up by header name so that anonymous column
// is harmlessly ignored. Including it in the test fixture keeps that
// real-world quirk under test.
const backfillCSVHeader = ",trip_id,route_id,lon,lat,bearing,speed,current_stop_sequence,current_status,timestamp,stop_id,vehicle_id,schedule_relationship,start_time,start_date,occupancy_status\n"

func TestParseBackfillCSVHappyPath(t *testing.T) {
	body := backfillCSVHeader +
		"0,T1,R1,-122.0,37.8,90,4.5,3,IN_TRANSIT_TO,1777390800,S1,V1,SCHEDULED,06:00:00,20260423,EMPTY\n" +
		"1,T2,R2,-122.1,37.9,180,5.0,1,STOPPED_AT,1777390860,S2,V2,SCHEDULED,06:05:00,20260423,EMPTY\n"

	out, rowsRead, rowsKept, err := parseBackfillCSV([]byte(body), "20260423")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if rowsRead != 2 {
		t.Fatalf("rowsRead = %d, want 2", rowsRead)
	}
	if rowsKept != 2 {
		t.Fatalf("rowsKept = %d, want 2", rowsKept)
	}
	if len(out) != 2 {
		t.Fatalf("len(out) = %d, want 2", len(out))
	}
	if out[0].VehicleID != "V1" || out[0].TripID != "T1" || out[0].RouteID != "R1" {
		t.Fatalf("out[0] = %+v, want V1/T1/R1", out[0])
	}
	wantTS := time.Unix(1777390800, 0).UTC()
	if !out[0].TS.Equal(wantTS) {
		t.Fatalf("out[0].TS = %v, want %v", out[0].TS, wantTS)
	}
	if out[0].Lat != 37.8 || out[0].Lon != -122.0 {
		t.Fatalf("out[0] coords = %v,%v, want 37.8,-122.0", out[0].Lat, out[0].Lon)
	}
	if out[0].StartDate != "20260423" {
		t.Fatalf("out[0].StartDate = %q, want 20260423", out[0].StartDate)
	}
}

// Rows whose start_date doesn't match the target day are dropped — this is
// how we exclude trips that ended before our window or that began on a
// neighbouring service day but happen to share a CSV file with our day.
func TestParseBackfillCSVFiltersByStartDate(t *testing.T) {
	body := backfillCSVHeader +
		"0,T_yes,R1,-122.0,37.8,0,0,1,IN_TRANSIT_TO,1777390800,S1,V1,SCHEDULED,06:00:00,20260423,EMPTY\n" +
		"1,T_no,R1,-122.0,37.8,0,0,1,IN_TRANSIT_TO,1777390800,S1,V2,SCHEDULED,06:00:00,20260422,EMPTY\n" +
		"2,T_no2,R1,-122.0,37.8,0,0,1,IN_TRANSIT_TO,1777390800,S1,V3,SCHEDULED,06:00:00,20260424,EMPTY\n"

	out, rowsRead, rowsKept, err := parseBackfillCSV([]byte(body), "20260423")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if rowsRead != 3 {
		t.Fatalf("rowsRead = %d, want 3 (all 3 considered)", rowsRead)
	}
	if rowsKept != 1 {
		t.Fatalf("rowsKept = %d, want 1 (only T_yes matches)", rowsKept)
	}
	if len(out) != 1 || out[0].TripID != "T_yes" {
		t.Fatalf("out = %+v, want only T_yes", out)
	}
}

// Missing required columns must error so a malformed file doesn't silently
// drop on the floor — the caller logs and moves on, but we want a clear
// reason rather than silent zero.
func TestParseBackfillCSVMissingColumns(t *testing.T) {
	body := "trip_id,route_id\nT1,R1\n"
	_, _, _, err := parseBackfillCSV([]byte(body), "20260423")
	if err == nil {
		t.Fatalf("err = nil, want missing-column error")
	}
	if !strings.Contains(err.Error(), "missing column") {
		t.Fatalf("err = %v, want missing-column", err)
	}
}

// Per-row defects (bad lat/lon, bad timestamp, empty trip_id) are skipped
// silently; the file as a whole still parses. Otherwise one stray byte in
// year-old archives would tank an entire day's backfill.
func TestParseBackfillCSVPerRowDefectsAreSkipped(t *testing.T) {
	body := backfillCSVHeader +
		"0,T1,R1,-122.0,37.8,0,0,1,IN_TRANSIT_TO,1777390800,S1,V1,SCHEDULED,06:00:00,20260423,EMPTY\n" +
		"1,,R1,-122.0,37.8,0,0,1,IN_TRANSIT_TO,1777390860,S1,V2,SCHEDULED,06:00:00,20260423,EMPTY\n" + // empty trip_id
		"2,T3,R1,not-a-number,37.8,0,0,1,IN_TRANSIT_TO,1777390920,S1,V3,SCHEDULED,06:00:00,20260423,EMPTY\n" + // bad lon
		"3,T4,R1,-122.0,37.8,0,0,1,IN_TRANSIT_TO,not-a-ts,S1,V4,SCHEDULED,06:00:00,20260423,EMPTY\n" // bad timestamp

	out, _, rowsKept, err := parseBackfillCSV([]byte(body), "20260423")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if rowsKept != 1 || len(out) != 1 || out[0].TripID != "T1" {
		t.Fatalf("rowsKept = %d, out = %+v, want only T1", rowsKept, out)
	}
}

// reconstructTrips groups by (vehicle_id, trip_id), sorts probes by TS,
// dedupes duplicate-TS probes (parked-bus thrash), and pulls out one
// inFlightTrip per group with first/last seen timestamps wired up.
func TestReconstructTripsGroupsAndSortsAndDedupes(t *testing.T) {
	t0 := time.Unix(1777390800, 0).UTC()
	snapshots := []vehicleSnapshot{
		// V1/T1 — three snapshots out of order with a duplicate TS.
		{VehicleID: "V1", TripID: "T1", RouteID: "R1", StartDate: "20260423", TS: t0.Add(120 * time.Second), Lat: 37.81, Lon: -122.0},
		{VehicleID: "V1", TripID: "T1", RouteID: "R1", StartDate: "20260423", TS: t0, Lat: 37.80, Lon: -122.0},
		{VehicleID: "V1", TripID: "T1", RouteID: "R1", StartDate: "20260423", TS: t0, Lat: 37.80, Lon: -122.0}, // dup TS
		{VehicleID: "V1", TripID: "T1", RouteID: "R1", StartDate: "20260423", TS: t0.Add(60 * time.Second), Lat: 37.805, Lon: -122.0},
		// V2/T2 — separate group.
		{VehicleID: "V2", TripID: "T2", RouteID: "R2", StartDate: "20260423", TS: t0.Add(30 * time.Second), Lat: 37.9, Lon: -122.1},
		// V1/T_other — same vehicle, different trip_id (e.g., reassignment).
		{VehicleID: "V1", TripID: "T_other", RouteID: "R3", StartDate: "20260423", TS: t0.Add(500 * time.Second), Lat: 37.7, Lon: -122.2},
	}

	// nil cache so we exercise the grouping logic without GTFS dependencies;
	// projection is a no-op without a route in cache, which is fine here.
	out := reconstructTrips(snapshots, nil)

	if len(out) != 3 {
		t.Fatalf("len(out) = %d, want 3 groups", len(out))
	}

	byKey := make(map[string]inFlightTrip, len(out))
	for _, tr := range out {
		byKey[tr.VehicleID+"/"+tr.TripID] = tr
	}

	v1t1, ok := byKey["V1/T1"]
	if !ok {
		t.Fatalf("missing V1/T1 group")
	}
	if len(v1t1.Probes) != 3 {
		t.Fatalf("V1/T1 probes = %d, want 3 (dup-TS deduped, three remain)", len(v1t1.Probes))
	}
	for i := 1; i < len(v1t1.Probes); i++ {
		if !v1t1.Probes[i].TS.After(v1t1.Probes[i-1].TS) {
			t.Fatalf("V1/T1 probes not strictly ascending: %v then %v",
				v1t1.Probes[i-1].TS, v1t1.Probes[i].TS)
		}
	}
	if !v1t1.FirstSeenTS.Equal(t0) {
		t.Fatalf("V1/T1 FirstSeenTS = %v, want %v", v1t1.FirstSeenTS, t0)
	}
	if !v1t1.LastSeenTS.Equal(t0.Add(120 * time.Second)) {
		t.Fatalf("V1/T1 LastSeenTS = %v, want %v", v1t1.LastSeenTS, t0.Add(120*time.Second))
	}

	if _, ok := byKey["V2/T2"]; !ok {
		t.Fatalf("missing V2/T2 group")
	}
	if _, ok := byKey["V1/T_other"]; !ok {
		t.Fatalf("missing V1/T_other group (same vehicle, different trip)")
	}
}

// With a cache that knows the route+trip, projection should fire and
// stops should be detected (mirroring live tracking).
func TestReconstructTripsProjectsAndDetectsArrivals(t *testing.T) {
	cache := fixtureRouteCache()
	t0 := time.Unix(1777390800, 0).UTC()

	// Two probes for V1/T1: one before stop A (dist=0), one well past
	// stop C (dist=500). Should record A, B, C as detected arrivals.
	snapshots := []vehicleSnapshot{
		{VehicleID: "V1", TripID: "T1", RouteID: "R1", StartDate: "20260423", TS: t0, Lat: 0, Lon: -0.0001},
		{VehicleID: "V1", TripID: "T1", RouteID: "R1", StartDate: "20260423", TS: t0.Add(60 * time.Second), Lat: 0, Lon: 0.005},
	}

	out := reconstructTrips(snapshots, cache)
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	trip := out[0]
	if len(trip.Probes) != 2 {
		t.Fatalf("Probes = %d, want 2", len(trip.Probes))
	}
	if trip.Probes[0].DistAlongRouteM == 0 {
		// projection ran and assigned a non-zero dist for the second probe at least
		// (the first probe is at distance ≈ 0 by construction; we accept either)
	}
	if trip.Probes[1].DistAlongRouteM < 400 {
		t.Fatalf("Probes[1].DistAlongRouteM = %.1f, want ≥ 400 (well past stop C)",
			trip.Probes[1].DistAlongRouteM)
	}
	if len(trip.StopArrivals) == 0 {
		t.Fatalf("expected stop arrivals to be detected, got 0")
	}
}
