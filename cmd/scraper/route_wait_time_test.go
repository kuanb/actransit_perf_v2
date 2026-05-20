package main

import (
	"math"
	"testing"
)

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
	if got := closedFormMeanWaitFromMass(make([]float64, waitHistBins)); got != 0 {
		t.Errorf("mean wait of empty mass: want 0, got %v", got)
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
