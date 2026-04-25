package main

import (
	"math"
	"testing"
)

func TestHaversineMeters(t *testing.T) {
	cases := []struct {
		name                   string
		lat1, lon1, lat2, lon2 float64
		want                   float64
		tolMeters              float64
	}{
		{"same point", 37.0, -122.0, 37.0, -122.0, 0, 0.001},
		{"one milli-degree lon at equator", 0, 0, 0, 0.001, 111.32, 0.5},
		{"NYC to LA", 40.7128, -74.0060, 34.0522, -118.2437, 3_944_000, 20_000},
		// Antimeridian: (0, 179) -> (0, -179) is 2° on the equator. With
		// earthRadiusM = 6371000, the great-circle distance is
		// 6371000 * 2 * π/180 ≈ 222390 m. The formula must treat this as
		// a short distance, not 358°.
		{"across antimeridian", 0, 179, 0, -179, 222_390, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := haversineMeters(tc.lat1, tc.lon1, tc.lat2, tc.lon2)
			if math.Abs(got-tc.want) > tc.tolMeters {
				t.Fatalf("haversineMeters = %.3f, want %.3f (±%.3f)", got, tc.want, tc.tolMeters)
			}
		})
	}
}

func TestProjectPointOntoSegment(t *testing.T) {
	const eps = 1e-9

	t.Run("point on segment midpoint", func(t *testing.T) {
		tParam, distSq := projectPointOntoSegment(0.5, 0, 0, 0, 1, 0)
		if math.Abs(tParam-0.5) > eps {
			t.Fatalf("t = %v, want 0.5", tParam)
		}
		if distSq > eps {
			t.Fatalf("distSq = %v, want 0", distSq)
		}
	})

	t.Run("point off before-end clamps to t=0", func(t *testing.T) {
		tParam, distSq := projectPointOntoSegment(-1, 0, 0, 0, 1, 0)
		if tParam != 0 {
			t.Fatalf("t = %v, want 0 (clamped)", tParam)
		}
		if math.Abs(distSq-1) > eps {
			t.Fatalf("distSq = %v, want 1", distSq)
		}
	})

	t.Run("point off after-end clamps to t=1", func(t *testing.T) {
		tParam, distSq := projectPointOntoSegment(2, 0, 0, 0, 1, 0)
		if tParam != 1 {
			t.Fatalf("t = %v, want 1 (clamped)", tParam)
		}
		if math.Abs(distSq-1) > eps {
			t.Fatalf("distSq = %v, want 1", distSq)
		}
	})

	t.Run("zero-length segment uses fallback distance", func(t *testing.T) {
		tParam, distSq := projectPointOntoSegment(3, 4, 0, 0, 0, 0)
		if tParam != 0 {
			t.Fatalf("t = %v, want 0", tParam)
		}
		// distSq from (0,0) to (3,4) is 25.
		if math.Abs(distSq-25) > eps {
			t.Fatalf("distSq = %v, want 25", distSq)
		}
	})

	t.Run("perpendicular offset off middle of segment", func(t *testing.T) {
		// Segment along x-axis from (0,0) to (10,0), point at (5,3).
		// Foot is (5,0); distSq = 9; t = 0.5.
		tParam, distSq := projectPointOntoSegment(5, 3, 0, 0, 10, 0)
		if math.Abs(tParam-0.5) > eps {
			t.Fatalf("t = %v, want 0.5", tParam)
		}
		if math.Abs(distSq-9) > eps {
			t.Fatalf("distSq = %v, want 9", distSq)
		}
	})
}

func TestProjectLatLonOntoShape(t *testing.T) {
	// Straight east-west shape at the equator. cos(0)=1 so
	// metersPerDegLon ≈ 111320. Cumulative distances chosen to match what
	// the caller would compute via haversineMeters.
	shape := [][3]float64{
		{0, 0.000, 0},
		{0, 0.001, 111.32},
		{0, 0.002, 222.64},
		{0, 0.003, 333.96},
	}

	t.Run("point exactly on shape midpoint", func(t *testing.T) {
		// (0, 0.0015): t=0.5 along segment 1-2, perpDist=0.
		distAlong, perpDist := projectLatLonOntoShape(0, 0.0015, shape)
		wantAlong := 111.32 + 0.5*(222.64-111.32)
		if math.Abs(distAlong-wantAlong) > 1.0 {
			t.Fatalf("distAlong = %.3f, want %.3f (±1m)", distAlong, wantAlong)
		}
		if perpDist > 0.5 {
			t.Fatalf("perpDist = %.3f, want ≈0", perpDist)
		}
	})

	t.Run("point offset perpendicular to shape", func(t *testing.T) {
		// Offset 0.0001° lat north of (0, 0.0015). 1° lat = 110540 m so
		// 0.0001° ≈ 11.054 m. distAlong should still be the foot.
		distAlong, perpDist := projectLatLonOntoShape(0.0001, 0.0015, shape)
		wantAlong := 111.32 + 0.5*(222.64-111.32)
		if math.Abs(distAlong-wantAlong) > 1.0 {
			t.Fatalf("distAlong = %.3f, want %.3f (±1m)", distAlong, wantAlong)
		}
		if math.Abs(perpDist-11.054) > 0.5 {
			t.Fatalf("perpDist = %.3f, want ≈11.054 (±0.5m)", perpDist)
		}
	})

	t.Run("degenerate shape with one point returns zeroes", func(t *testing.T) {
		distAlong, perpDist := projectLatLonOntoShape(1, 1, [][3]float64{{0, 0, 0}})
		if distAlong != 0 || perpDist != 0 {
			t.Fatalf("got (%v, %v), want (0, 0)", distAlong, perpDist)
		}
	})

	t.Run("empty shape returns zeroes", func(t *testing.T) {
		distAlong, perpDist := projectLatLonOntoShape(1, 1, nil)
		if distAlong != 0 || perpDist != 0 {
			t.Fatalf("got (%v, %v), want (0, 0)", distAlong, perpDist)
		}
	})
}
