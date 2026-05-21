package main

import (
	"testing"
)

func makeBins(pairs ...[2]int) []int64 {
	out := make([]int64, speedBinCount)
	for _, p := range pairs {
		out[p[0]] = int64(p[1])
	}
	return out
}

// Degenerate single-bin case: all legs at one speed bin.
// Mean = midpoint of that bin, stddev = 0, all percentiles within the bin.
func TestSummarizeSpeed_SingleBin(t *testing.T) {
	bins := makeBins([2]int{20, 100}) // bin 20 = [10.0, 10.5) mph; midpoint 10.25
	s := summarizeSpeedHistogramSummary(bins)

	if s.N != 100 {
		t.Fatalf("N: got %d, want 100", s.N)
	}
	// round1 quantizes to 1 decimal, so the midpoint 10.25 rounds to 10.3.
	if s.MeanMph == nil || !approxEqual(*s.MeanMph, 10.3, 1e-6) {
		t.Errorf("mean: got %v, want 10.3 (round1 of 10.25)", s.MeanMph)
	}
	if s.StddevMph == nil || !approxEqual(*s.StddevMph, 0.0, 1e-6) {
		t.Errorf("stddev: got %v, want 0.0", s.StddevMph)
	}
	// Percentiles all fall inside the single populated bin [10.0, 10.5).
	for label, ptr := range map[string]*float64{
		"p50": s.P50Mph, "p95": s.P95Mph, "p99": s.P99Mph,
	} {
		if ptr == nil {
			t.Errorf("%s: nil", label)
			continue
		}
		if *ptr < 10.0 || *ptr > 10.5 {
			t.Errorf("%s: %v out of bin [10.0, 10.5)", label, *ptr)
		}
	}
}

// Two-mode distribution: bins 0..9 each carry 100 legs (so 1000 legs in
// [0, 5) mph, uniformly distributed across bins) plus bins 80..89 each
// carry 100 legs (1000 legs in [40, 45) mph). Total N = 2000.
// Expected: mean ~= 22.5; p50 should fall right between the two clumps;
// p95/p99 land deep in the upper clump.
func TestSummarizeSpeed_BimodalDistribution(t *testing.T) {
	bins := make([]int64, speedBinCount)
	for i := 0; i < 10; i++ {
		bins[i] = 100
	}
	for i := 80; i < 90; i++ {
		bins[i] = 100
	}
	s := summarizeSpeedHistogramSummary(bins)

	if s.N != 2000 {
		t.Fatalf("N: got %d, want 2000", s.N)
	}
	// Mean = average of bin midpoints, weighted equally across all 2000
	// legs. Lower clump midpoints average 2.5; upper clump averages 42.5;
	// overall mean = 22.5.
	if s.MeanMph == nil || !approxEqual(*s.MeanMph, 22.5, 1e-1) {
		t.Errorf("mean: got %v, want ~22.5", s.MeanMph)
	}
	// p50 sits at the boundary between clumps: cumulative hits 1000 at
	// the upper edge of bin 9 (5.0 mph), so the median should be exactly
	// at that bin edge.
	if s.P50Mph == nil || !approxEqual(*s.P50Mph, 5.0, 0.5) {
		t.Errorf("p50: got %v, want near 5.0", s.P50Mph)
	}
	// p95 lands deep in the upper clump: target = 1900, cumulative passes
	// it within bin 88 → midpoint ~= 44.25 mph.
	if s.P95Mph == nil || *s.P95Mph < 43.0 || *s.P95Mph > 45.0 {
		t.Errorf("p95: got %v, want in [43.0, 45.0]", s.P95Mph)
	}
	// p99 sits near the top of the upper clump.
	if s.P99Mph == nil || *s.P99Mph < 44.0 || *s.P99Mph > 45.0 {
		t.Errorf("p99: got %v, want in [44.0, 45.0]", s.P99Mph)
	}
	// Variance is well above zero with two well-separated modes.
	if s.StddevMph == nil || *s.StddevMph < 15.0 {
		t.Errorf("stddev: got %v, expected at least 15.0 for bimodal", s.StddevMph)
	}
}

// Empty histogram yields zero N and nil summary fields.
func TestSummarizeSpeed_Empty(t *testing.T) {
	bins := make([]int64, speedBinCount)
	s := summarizeSpeedHistogramSummary(bins)
	if s.N != 0 {
		t.Errorf("N: got %d, want 0", s.N)
	}
	if s.MeanMph != nil || s.StddevMph != nil || s.P50Mph != nil || s.P95Mph != nil || s.P99Mph != nil {
		t.Errorf("expected all summary fields nil; got %+v", s)
	}
}

// percentileFromBins clamps the cumulative interpolation to within
// [0, 1] inside the crossing bin even with weird edge counts.
func TestSpeedPercentileFromBins_Boundaries(t *testing.T) {
	bins := makeBins([2]int{10, 50}, [2]int{50, 50})
	if v, ok := speedPercentileFromBins(bins, 100, 0.0); !ok || v < 0 || v > 6 {
		t.Errorf("p0: got %v ok=%v", v, ok)
	}
	if v, ok := speedPercentileFromBins(bins, 100, 1.0); !ok || v < 25 {
		t.Errorf("p100: got %v ok=%v, expected up in the higher bin", v, ok)
	}
}

// buildSpeedDayBlock drops cells with fewer than minLegsForHourCell legs
// but still folds their counts into the overall summary. With one
// "fat" hour and one "thin" hour, the by_hour array should have exactly
// one entry but the summary's N should equal the sum across hours.
func TestBuildSpeedDayBlock_DropsThinHours(t *testing.T) {
	fat := makeBins([2]int{20, int(minLegsForHourCell) + 5})
	thin := makeBins([2]int{30, int(minLegsForHourCell) - 1})
	byHour := map[int][]int64{
		8:  fat,
		23: thin,
	}
	block := buildSpeedDayBlock(byHour)

	if len(block.ByHour) != 1 {
		t.Fatalf("by_hour: got %d cells, want 1 (thin hour should be dropped)", len(block.ByHour))
	}
	if block.ByHour[0].Hour != 8 {
		t.Errorf("by_hour cell hour: got %d, want 8", block.ByHour[0].Hour)
	}
	wantN := int64(minLegsForHourCell) + 5 + int64(minLegsForHourCell) - 1
	if block.Summary.N != wantN {
		t.Errorf("summary N: got %d, want %d (sum across hours including dropped ones)", block.Summary.N, wantN)
	}
}

// lookupStopPair resolves a stop_sequence to its preceding stop_id
// within a trip's stop_times. The first stop has no predecessor so
// the leg arriving at it is undefined.
func TestLookupStopPair(t *testing.T) {
	trip := gtfsTrip{
		StopTimes: []gtfsStopTime{
			{StopSequence: 1, StopID: "A"},
			{StopSequence: 5, StopID: "B"}, // non-contiguous sequences are valid
			{StopSequence: 9, StopID: "C"},
		},
	}

	if _, ok := lookupStopPair(trip, 1); ok {
		t.Errorf("first stop should have no predecessor, got ok=true")
	}
	pair, ok := lookupStopPair(trip, 5)
	if !ok || pair.FromStopID != "A" || pair.ToStopID != "B" {
		t.Errorf("stop_seq 5: got %+v ok=%v, want {A,B}", pair, ok)
	}
	pair, ok = lookupStopPair(trip, 9)
	if !ok || pair.FromStopID != "B" || pair.ToStopID != "C" {
		t.Errorf("stop_seq 9: got %+v ok=%v, want {B,C}", pair, ok)
	}
	if _, ok := lookupStopPair(trip, 99); ok {
		t.Errorf("unknown stop_seq should not resolve")
	}
}

// aggregateRouteSegmentSpeedRows folds bins across trips that share
// the same (from_stop, to_stop) pair, routes correctly by direction,
// and skips trips whose first stop_sequence has no predecessor.
func TestAggregateRouteSegmentSpeedRows(t *testing.T) {
	cache := &gtfsCache{
		Routes: map[string]*processedGTFSRoute{
			"R1": {
				RouteID: "R1",
				Trips: map[string]gtfsTrip{
					// Two outbound trips with overlapping consecutive pairs:
					// trip_a stops 1→2: A→B, 2→3: B→C
					// trip_b stops 1→2: A→B, 2→3: B→D (different second leg)
					"trip_a": {DirectionID: 0, StopTimes: []gtfsStopTime{
						{StopSequence: 1, StopID: "A"},
						{StopSequence: 2, StopID: "B"},
						{StopSequence: 3, StopID: "C"},
					}},
					"trip_b": {DirectionID: 0, StopTimes: []gtfsStopTime{
						{StopSequence: 1, StopID: "A"},
						{StopSequence: 2, StopID: "B"},
						{StopSequence: 3, StopID: "D"},
					}},
					// Inbound trip with the same A→B leg.
					"trip_c": {DirectionID: 1, StopTimes: []gtfsStopTime{
						{StopSequence: 1, StopID: "A"},
						{StopSequence: 2, StopID: "B"},
					}},
				},
			},
		},
	}

	rows := []routeSegmentSpeedHistRow{
		// trip_a, A→B: 5 legs in bin 20
		{RouteID: "R1", TripID: "trip_a", StopSequence: 2, Bin: 20, N: 5},
		// trip_b, A→B: 7 legs in bin 20 (same pair → merges)
		{RouteID: "R1", TripID: "trip_b", StopSequence: 2, Bin: 20, N: 7},
		// trip_a, B→C: 3 legs in bin 30
		{RouteID: "R1", TripID: "trip_a", StopSequence: 3, Bin: 30, N: 3},
		// trip_b, B→D: 2 legs in bin 30 (distinct pair)
		{RouteID: "R1", TripID: "trip_b", StopSequence: 3, Bin: 30, N: 2},
		// trip_c (inbound), A→B: 9 legs in bin 25
		{RouteID: "R1", TripID: "trip_c", StopSequence: 2, Bin: 25, N: 9},
		// First-stop row has no leg → silently dropped (not a miss).
		{RouteID: "R1", TripID: "trip_a", StopSequence: 1, Bin: 20, N: 100},
		// Unknown trip → counted as a miss.
		{RouteID: "R1", TripID: "trip_z", StopSequence: 2, Bin: 20, N: 100},
	}

	byRoute, skipped := aggregateRouteSegmentSpeedRows(rows, cache)
	if skipped != 1 {
		t.Errorf("skipped: got %d, want 1 (unknown trip only)", skipped)
	}

	dir0 := byRoute["R1"][0]
	if got := dir0[segmentPairKey{"A", "B"}][20]; got != 12 {
		t.Errorf("A→B dir0 bin20: got %d, want 12", got)
	}
	if got := dir0[segmentPairKey{"B", "C"}][30]; got != 3 {
		t.Errorf("B→C dir0 bin30: got %d, want 3", got)
	}
	if got := dir0[segmentPairKey{"B", "D"}][30]; got != 2 {
		t.Errorf("B→D dir0 bin30: got %d, want 2", got)
	}

	dir1 := byRoute["R1"][1]
	if got := dir1[segmentPairKey{"A", "B"}][25]; got != 9 {
		t.Errorf("A→B dir1 bin25: got %d, want 9", got)
	}
}

// buildSegmentSummaries emits a deterministic, sorted list of pairs
// with mean / p50 / stddev populated and zero-N pairs filtered out.
func TestBuildSegmentSummaries(t *testing.T) {
	pairs := map[segmentPairKey][]int64{
		{"A", "B"}: makeBins([2]int{20, 100}),         // mean ~10.25 mph → rounded 10.3
		{"C", "D"}: makeBins([2]int{40, 50}),          // mean ~20.25 mph
		{"X", "Y"}: make([]int64, speedBinCount),       // empty → dropped
	}
	out := buildSegmentSummaries(pairs)

	if len(out) != 2 {
		t.Fatalf("len: got %d, want 2 (empty pair should be dropped)", len(out))
	}
	if out[0].FromStopID != "A" || out[1].FromStopID != "C" {
		t.Errorf("order: got %v, want sorted by from_stop_id ASC", []string{out[0].FromStopID, out[1].FromStopID})
	}
	if out[0].MeanMph == nil || !approxEqual(*out[0].MeanMph, 10.3, 1e-6) {
		t.Errorf("A→B mean: got %v, want 10.3", out[0].MeanMph)
	}
	if out[0].StddevMph == nil || !approxEqual(*out[0].StddevMph, 0.0, 1e-6) {
		t.Errorf("A→B stddev: got %v, want 0.0", out[0].StddevMph)
	}
}

// aggregateRouteSpeedRows routes rows by direction via the gtfsCache,
// skips rows whose trip_id isn't in the cache, and discards out-of-range
// bin indices defensively.
func TestAggregateRouteSpeedRows_DirectionRouting(t *testing.T) {
	cache := &gtfsCache{
		Routes: map[string]*processedGTFSRoute{
			"R1": {
				RouteID: "R1",
				Trips: map[string]gtfsTrip{
					"trip_a": {TripID: "trip_a", DirectionID: 0},
					"trip_b": {TripID: "trip_b", DirectionID: 0},
					"trip_c": {TripID: "trip_c", DirectionID: 1},
				},
			},
		},
	}

	rows := []routeSpeedHistRow{
		{RouteID: "R1", TripID: "trip_a", DayType: "weekday", Hour: 8, Bin: 20, N: 5},
		{RouteID: "R1", TripID: "trip_b", DayType: "weekday", Hour: 8, Bin: 20, N: 7},
		{RouteID: "R1", TripID: "trip_c", DayType: "weekday", Hour: 8, Bin: 40, N: 3},
		{RouteID: "R1", TripID: "trip_c", DayType: "weekend", Hour: 9, Bin: 40, N: 2},
		// Unknown route → skipped.
		{RouteID: "R2", TripID: "trip_z", DayType: "weekday", Hour: 8, Bin: 20, N: 100},
		// Unknown trip on a known route → skipped.
		{RouteID: "R1", TripID: "trip_x", DayType: "weekday", Hour: 8, Bin: 20, N: 100},
		// Out-of-range bin on a known trip → silently dropped, not skipped.
		{RouteID: "R1", TripID: "trip_a", DayType: "weekday", Hour: 8, Bin: int64(speedBinCount + 5), N: 9},
	}

	byRoute, skipped := aggregateRouteSpeedRows(rows, cache)

	if skipped != 2 {
		t.Errorf("skipped: got %d, want 2 (unknown route + unknown trip)", skipped)
	}

	r1, ok := byRoute["R1"]
	if !ok {
		t.Fatalf("R1 missing from byRoute")
	}
	if _, ok := byRoute["R2"]; ok {
		t.Errorf("R2 should be missing (skipped)")
	}

	// Dir 0, weekday, hour 8: 5 (trip_a) + 7 (trip_b) = 12 legs in bin 20.
	d0wk8 := r1[0]["weekday"][8]
	if d0wk8[20] != 12 {
		t.Errorf("dir 0 weekday hour 8 bin 20: got %d, want 12", d0wk8[20])
	}
	// Dir 1, weekday, hour 8: 3 legs in bin 40.
	d1wk8 := r1[1]["weekday"][8]
	if d1wk8[40] != 3 {
		t.Errorf("dir 1 weekday hour 8 bin 40: got %d, want 3", d1wk8[40])
	}
	// Dir 1, weekend, hour 9: 2 legs in bin 40.
	d1we9 := r1[1]["weekend"][9]
	if d1we9[40] != 2 {
		t.Errorf("dir 1 weekend hour 9 bin 40: got %d, want 2", d1we9[40])
	}
}
