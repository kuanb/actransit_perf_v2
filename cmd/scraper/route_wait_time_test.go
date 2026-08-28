package main

import (
	"math"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func TestHeadwaysCTEUsesActualArrivalsWithinDirection(t *testing.T) {
	q := headwaysCTE(
		civil.Date{Year: 2026, Month: 8, Day: 24},
		civil.Date{Year: 2026, Month: 8, Day: 28},
	)
	if !strings.Contains(q, "PARTITION BY route_id, direction_id, stop_id, service_date") {
		t.Fatal("headways must partition arrivals by route, direction, stop, and service date")
	}
	if !strings.Contains(q, "actual_arrival IS NOT NULL AND direction_id IS NOT NULL") {
		t.Fatal("headways must use observed arrivals with a known direction")
	}
	if strings.Contains(q, "is_stale = FALSE") {
		t.Fatal("headways must retain actual arrivals from stale-finalized trips")
	}
}

func sumDensity(d []float64) float64 {
	s := 0.0
	for _, v := range d {
		s += v
	}
	return s
}

func approxEqual(a, b, tol float64) bool {
	return math.Abs(a-b) <= tol
}

// Single fixed headway H=10 min, 100 occurrences.
// Expected wait density: uniform on [0, 10), zero elsewhere.
// Mean wait = 5; median wait = 5.
func TestRouteWait_SingleFixedHeadway(t *testing.T) {
	heads := make([]float64, 100)
	for i := range heads {
		heads[i] = 10.0
	}
	mass := binMassFromHeadways(heads, waitHistBins)
	hist := densityFromMass(mass)

	if !approxEqual(sumDensity(hist.Density), 1.0, 5e-3) {
		t.Fatalf("density should sum to ~1.0; got %v", sumDensity(hist.Density))
	}
	for i := 0; i < 10; i++ {
		if !approxEqual(hist.Density[i], 0.1, 1e-6) {
			t.Errorf("bin %d: want 0.1, got %v", i, hist.Density[i])
		}
	}
	for i := 10; i < waitHistBins; i++ {
		if hist.Density[i] != 0 {
			t.Errorf("bin %d should be 0; got %v", i, hist.Density[i])
		}
	}

	mean := closedFormMeanWaitFromMass(mass)
	if !approxEqual(mean, 5.0, 1e-6) {
		t.Errorf("mean wait: want 5.0, got %v", mean)
	}

	median, ok := medianFromDensity(hist)
	if !ok {
		t.Fatal("medianFromDensity returned !ok for non-empty density")
	}
	if !approxEqual(median, 5.0, 1e-6) {
		t.Errorf("median wait: want 5.0, got %v", median)
	}
}

// Mixed headways {5, 5, 5, 30}.
// E[H] = 45/4 = 11.25; E[H^2] = (3*25 + 900)/4 = 243.75.
// Closed-form mean wait = E[H^2]/(2 E[H]) = 487.5 / 22.5 = 21.667? Let me recompute.
// Actually E[H^2]/(2 E[H]) with the sample second moment:
//
//	mean_wait = SUM(h^2) / (2 * SUM(h)) = (3*25 + 900) / (2 * (3*5 + 30))
//	          = 975 / 90 = 10.8333
//
// Median wait: F_W(w) = (1/E[H]) ∫_0^w (1 - F_H(u)) du, expected 7.5.
func TestRouteWait_MixedHeadways(t *testing.T) {
	heads := []float64{5, 5, 5, 30}
	mass := binMassFromHeadways(heads, waitHistBins)

	hist := densityFromMass(mass)
	// densityFromMass rounds each bin to 3 dp for compact JSON, so
	// the sum drifts by up to ~5e-3 across many small bins; the test
	// uses a tolerance that catches gross errors but accepts that.
	if !approxEqual(sumDensity(hist.Density), 1.0, 5e-3) {
		t.Fatalf("density should sum to ~1.0; got %v", sumDensity(hist.Density))
	}

	mean := closedFormMeanWaitFromMass(mass)
	wantMean := 975.0 / 90.0
	if !approxEqual(mean, wantMean, 1e-6) {
		t.Errorf("mean wait: want %v, got %v", wantMean, mean)
	}

	median, ok := medianFromDensity(hist)
	if !ok {
		t.Fatal("medianFromDensity returned !ok for non-empty density")
	}
	if !approxEqual(median, 7.5, 0.1) {
		t.Errorf("median wait: want 7.5, got %v", median)
	}
}

func TestRouteWait_EmptyInput(t *testing.T) {
	hist := densityFromMass(make([]float64, waitHistBins))
	if _, ok := medianFromDensity(hist); ok {
		t.Errorf("medianFromDensity should return !ok for zero density")
	}
	if _, ok := percentileFromDensity(hist, 0.95); ok {
		t.Errorf("percentileFromDensity(0.95) should return !ok for zero density")
	}
	if got := closedFormMeanWaitFromMass(make([]float64, waitHistBins)); got != 0 {
		t.Errorf("mean wait of empty mass: want 0, got %v", got)
	}
}

func TestRouteWait_ExpectedWaitSplitsHeadwaysAtHourBoundaries(t *testing.T) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	arrival := func(hour, minute int) time.Time {
		return time.Date(2026, 8, 4, hour, minute, 0, 0, loc)
	}
	arrivals := []time.Time{
		arrival(0, 55),
		arrival(1, 10),
		arrival(1, 20),
		arrival(1, 50),
		arrival(2, 10),
	}
	var hours [24]hourlyWaitAccumulator
	accumulateExpectedWaitByHour(&hours, arrivals)

	if got := hours[1].N; got != 4 {
		t.Fatalf("1am contributing headways: got %d, want 4", got)
	}
	if got := hours[1].CoveredSeconds; got != 60*60 {
		t.Fatalf("1am covered seconds: got %.0f, want 3600", got)
	}
	got := hours[1].WaitArea / hours[1].CoveredSeconds / 60
	if !approxEqual(got, 11.6667, 0.001) {
		t.Fatalf("1am expected wait: got %.4f, want 11.6667", got)
	}

	got = hours[0].WaitArea / hours[0].CoveredSeconds / 60
	if !approxEqual(got, 12.5, 0.001) {
		t.Fatalf("12am boundary contribution: got %.4f, want 12.5", got)
	}
	got = hours[2].WaitArea / hours[2].CoveredSeconds / 60
	if !approxEqual(got, 5.0, 0.001) {
		t.Fatalf("2am boundary contribution: got %.4f, want 5.0", got)
	}
}

// Uniform headway H=10 → wait density uniform on [0, 10) with mass 0.1
// per 1-min bin. Cumulative crosses p at exactly w = 10 * p, so:
//
//	p95 = 9.5  (cumulative 0.9 after bin 9, +0.5 of bin 9's mass)
//	p99 = 9.9  (cumulative 0.9 after bin 9, +0.9 of bin 9's mass)
func TestRouteWait_PercentileFromDensityUniform(t *testing.T) {
	heads := make([]float64, 100)
	for i := range heads {
		heads[i] = 10.0
	}
	hist := densityFromMass(binMassFromHeadways(heads, waitHistBins))

	p95, ok := percentileFromDensity(hist, 0.95)
	if !ok {
		t.Fatal("percentileFromDensity(0.95) returned !ok")
	}
	if !approxEqual(p95, 9.5, 0.05) {
		t.Errorf("p95 wait: want ~9.5, got %v", p95)
	}

	p99, ok := percentileFromDensity(hist, 0.99)
	if !ok {
		t.Fatal("percentileFromDensity(0.99) returned !ok")
	}
	if !approxEqual(p99, 9.9, 0.05) {
		t.Errorf("p99 wait: want ~9.9, got %v", p99)
	}
}

// medianFromDensity must agree with percentileFromDensity(h, 0.5)
// since the former is now a wrapper around the latter.
func TestRouteWait_PercentileWrapperMatchesMedian(t *testing.T) {
	heads := []float64{5, 5, 5, 30}
	hist := densityFromMass(binMassFromHeadways(heads, waitHistBins))

	med, mok := medianFromDensity(hist)
	p50, pok := percentileFromDensity(hist, 0.5)
	if mok != pok {
		t.Fatalf("ok flag mismatch: median=%v p50=%v", mok, pok)
	}
	if med != p50 {
		t.Errorf("medianFromDensity (%v) != percentileFromDensity(0.5) (%v)", med, p50)
	}
}

// Density should round-trip an exponential headway: with H ~ Exp(λ),
// the wait time is also Exp(λ), so mean wait = 1/λ = mean headway.
// Sample-based check with mild tolerance.
func TestRouteWait_ExponentialHeadways(t *testing.T) {
	// Inverse-CDF sample: u_i ∈ (0, 1), h_i = -ln(1-u_i) / λ with
	// λ = 1/10 (so E[H] = 10). Use a deterministic equi-spaced
	// sample to keep the test reproducible.
	lambda := 0.1
	n := 4000
	heads := make([]float64, n)
	for i := 0; i < n; i++ {
		u := (float64(i) + 0.5) / float64(n) // deterministic quantile
		heads[i] = -math.Log(1-u) / lambda
		if heads[i] > waitHistBins-1 {
			heads[i] = waitHistBins - 1
		}
	}
	mass := binMassFromHeadways(heads, waitHistBins)
	mean := closedFormMeanWaitFromMass(mass)
	if !approxEqual(mean, 10.0, 1.0) {
		t.Errorf("exponential mean wait: want ~10, got %v", mean)
	}
}
