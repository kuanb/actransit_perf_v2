package main

import (
	"archive/zip"
	"bytes"
	"testing"
)

// Synthetic GTFS feed used by the tests. Two routes (R1, R2) sharing one
// stop (A). T1 and T2 are on R1 and share shape S1 — that gives us a
// shared (shape, stop) pair to verify projection consistency across trips.
const (
	syntheticStopsTxt = `stop_id,stop_name,stop_lat,stop_lon
A,Stop A,37.8,-122.000
B,Stop B,37.8,-121.995
C,Stop C,37.8,-121.990
D,Stop D,37.8,-121.980
`
	syntheticShapesTxt = `shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon
S1,1,37.8,-122.000
S1,2,37.8,-121.990
S1,3,37.8,-121.980
S1,4,37.8,-121.970
S2,1,37.8,-122.000
S2,2,37.8,-121.985
S2,3,37.8,-121.970
`
	syntheticTripsTxt = `trip_id,route_id,shape_id,service_id,direction_id
T1,R1,S1,SVC,0
T2,R1,S1,SVC,0
T3,R2,S2,SVC,0
`
	syntheticStopTimesTxt = `trip_id,stop_sequence,stop_id,arrival_time,departure_time
T1,1,A,08:00:00,08:00:00
T1,2,B,08:01:00,08:01:00
T1,3,C,08:02:00,08:02:00
T1,4,D,08:03:00,08:03:00
T2,1,A,09:00:00,09:00:00
T2,2,C,09:01:00,09:01:00
T2,3,D,09:02:00,09:02:00
T3,1,A,10:00:00,10:00:00
T3,2,D,10:01:00,10:01:00
`
)

func buildSyntheticGTFSZip(t *testing.T, mutate func(map[string]string)) []byte {
	t.Helper()
	files := map[string]string{
		"stops.txt":      syntheticStopsTxt,
		"shapes.txt":     syntheticShapesTxt,
		"trips.txt":      syntheticTripsTxt,
		"stop_times.txt": syntheticStopTimesTxt,
	}
	if mutate != nil {
		mutate(files)
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func TestProcessGTFSHappyPath(t *testing.T) {
	routes, err := processGTFS(buildSyntheticGTFSZip(t, nil), "test-hash")
	if err != nil {
		t.Fatalf("processGTFS: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("got %d routes, want 2", len(routes))
	}
	for _, want := range []string{"R1", "R2"} {
		if _, ok := routes[want]; !ok {
			t.Fatalf("missing route %q", want)
		}
	}

	r1 := routes["R1"]
	if r1.FeedHash != "test-hash" {
		t.Fatalf("R1.FeedHash = %q, want test-hash", r1.FeedHash)
	}
	if len(r1.Trips) != 2 {
		t.Fatalf("R1 has %d trips, want 2", len(r1.Trips))
	}
	for _, tid := range []string{"T1", "T2"} {
		if _, ok := r1.Trips[tid]; !ok {
			t.Fatalf("R1 missing trip %q", tid)
		}
	}
	for _, sid := range []string{"A", "B", "C", "D"} {
		if _, ok := r1.Stops[sid]; !ok {
			t.Fatalf("R1 missing stop %q", sid)
		}
	}
	if _, ok := r1.Shapes["S1"]; !ok {
		t.Fatalf("R1 missing shape S1")
	}

	r2 := routes["R2"]
	if len(r2.Trips) != 1 {
		t.Fatalf("R2 has %d trips, want 1", len(r2.Trips))
	}
	if _, ok := r2.Trips["T3"]; !ok {
		t.Fatalf("R2 missing trip T3")
	}
}

func TestProcessGTFSStopTimeProjectionsMonotonic(t *testing.T) {
	routes, err := processGTFS(buildSyntheticGTFSZip(t, nil), "h")
	if err != nil {
		t.Fatalf("processGTFS: %v", err)
	}
	t1 := routes["R1"].Trips["T1"]
	if len(t1.StopTimes) != 4 {
		t.Fatalf("T1 has %d stop_times, want 4", len(t1.StopTimes))
	}
	prev := -1.0
	for i, st := range t1.StopTimes {
		if i > 0 && !(st.DistAlongRoute > prev) {
			t.Fatalf("T1 stop_times[%d] DistAlongRoute=%.3f not > prev=%.3f",
				i, st.DistAlongRoute, prev)
		}
		prev = st.DistAlongRoute
	}
	// Stop B is mid-segment-1 on S1 — must be strictly positive.
	if t1.StopTimes[1].DistAlongRoute <= 0 {
		t.Fatalf("T1 stop B DistAlongRoute=%.3f, want > 0",
			t1.StopTimes[1].DistAlongRoute)
	}
}

// Memoization correctness proxy: T1 and T2 share shape S1 and both visit
// stops A, C, D. The projection values must agree across the two trips.
// If memoization were broken (e.g., recomputed per trip with state mutation
// in between), this is the assertion that would fail.
func TestProcessGTFSSharedShapeStopProjectionsMatch(t *testing.T) {
	routes, err := processGTFS(buildSyntheticGTFSZip(t, nil), "h")
	if err != nil {
		t.Fatalf("processGTFS: %v", err)
	}
	t1 := routes["R1"].Trips["T1"]
	t2 := routes["R1"].Trips["T2"]

	distFor := func(sts []gtfsStopTime, stopID string) float64 {
		for _, st := range sts {
			if st.StopID == stopID {
				return st.DistAlongRoute
			}
		}
		t.Fatalf("stop %q not found", stopID)
		return 0
	}
	for _, sid := range []string{"A", "C", "D"} {
		d1 := distFor(t1.StopTimes, sid)
		d2 := distFor(t2.StopTimes, sid)
		if d1 != d2 {
			t.Fatalf("stop %q on shared shape: T1=%.6f T2=%.6f (want equal)",
				sid, d1, d2)
		}
	}
}

// Regression test for the BOM-in-source bug. If gtfs_static.go's BOM strip
// is removed, stop_id resolves to "" and stops are dropped from the output.
func TestProcessGTFSBOMInStopsHeader(t *testing.T) {
	z := buildSyntheticGTFSZip(t, func(m map[string]string) {
		m["stops.txt"] = "\uFEFF" + m["stops.txt"]
	})
	routes, err := processGTFS(z, "h")
	if err != nil {
		t.Fatalf("processGTFS with BOM: %v", err)
	}
	if len(routes["R1"].Stops) == 0 {
		t.Fatalf("R1 has no stops; BOM stripping likely failed")
	}
}

func TestSanitizeRouteID(t *testing.T) {
	cases := []struct{ in, out string }{
		{"R1", "R1"},
		{"abc-XYZ_123", "abc-XYZ_123"},
		{"with space", "with_space"},
		{"with/slash", "with_slash"},
		{"dot.dot", "dot_dot"},
		{"mix-1.2/3 four", "mix-1_2_3_four"},
		{"", ""},
	}
	for _, c := range cases {
		if got := sanitizeRouteID(c.in); got != c.out {
			t.Fatalf("sanitizeRouteID(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}
