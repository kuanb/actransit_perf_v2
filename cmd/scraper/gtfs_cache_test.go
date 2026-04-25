package main

import "testing"

func TestNearestStopSeq(t *testing.T) {
	stops := []gtfsStopTime{
		{StopSequence: 1, DistAlongRoute: 0},
		{StopSequence: 2, DistAlongRoute: 100},
		{StopSequence: 3, DistAlongRoute: 250},
		{StopSequence: 4, DistAlongRoute: 400},
	}
	cases := []struct {
		name string
		dist float64
		want int
	}{
		{"before first stop", -50, 1},
		{"exactly on stop 1", 0, 1},
		{"closer to stop 2", 90, 2},
		{"midpoint between 2 and 3 — first wins on tie", 175, 2},
		{"closer to stop 3", 200, 3},
		{"past last stop clamps to 4", 5000, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := nearestStopSeq(stops, tc.dist)
			if got != tc.want {
				t.Fatalf("nearestStopSeq(%.1f) = %d, want %d", tc.dist, got, tc.want)
			}
		})
	}

	t.Run("empty stops returns 0", func(t *testing.T) {
		if got := nearestStopSeq(nil, 100); got != 0 {
			t.Fatalf("got %d, want 0", got)
		}
	})
}
