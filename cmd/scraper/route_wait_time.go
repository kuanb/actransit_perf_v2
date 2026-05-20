package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	routeWaitWeeklyPrefix = "stats/weekly/route_wait/"

	// Headway bounds applied when deriving inter-arrival gaps from
	// successive actual_arrival values at the same (route, stop). 30 s
	// drops degenerate near-zero gaps that occasionally slip past the
	// trip_observations dedup (two different trip_ids finalizing on the
	// same stop within seconds of each other). 90 min cuts off the
	// overnight-shutdown gaps so they don't dominate the distribution.
	waitMinHeadwaySeconds = 30
	waitMaxHeadwaySeconds = 90 * 60

	// Wait-time histogram covers 0..waitHistBins minutes in 1-min bins.
	// The frontend shows bins [0, 60) and ignores the longer tail.
	waitHistBins = 90

	// Per-hour cells are dropped from the JSON when fewer than this
	// many headways fed them. Matches minObservationsForHourCell in
	// weekly.go (same justification: small samples produce wild
	// quantile noise that distorts the hourly line plot).
	minHeadwaysForHourCell = 5
)

type routeWaitSummary struct {
	N              int64    `json:"n"`
	MeanHeadwayMin *float64 `json:"mean_headway_min"`
	P50HeadwayMin  *float64 `json:"p50_headway_min"`
	MeanWaitMin    *float64 `json:"mean_wait_min"`
	MedianWaitMin  *float64 `json:"median_wait_min"`
	P95WaitMin     *float64 `json:"p95_wait_min"`
	P99WaitMin     *float64 `json:"p99_wait_min"`
}

type routeWaitHourCell struct {
	Hour          int      `json:"hour"`
	N             int64    `json:"n"`
	P50HeadwayMin *float64 `json:"p50_headway_min"`
	MeanWaitMin   *float64 `json:"mean_wait_min"`
	MedianWaitMin *float64 `json:"median_wait_min"`
	P95WaitMin    *float64 `json:"p95_wait_min"`
	P99WaitMin    *float64 `json:"p99_wait_min"`
}

type routeWaitHistogram struct {
	BinLoMin []int     `json:"bin_lo_min"`
	Density  []float64 `json:"density"`
}

type routeWaitDayBlock struct {
	Summary   routeWaitSummary    `json:"summary"`
	Histogram routeWaitHistogram  `json:"histogram"`
	ByHour    []routeWaitHourCell `json:"by_hour"`
}

type routeWaitWeeklyStats struct {
	RouteID string                       `json:"route_id"`
	WeekEnd string                       `json:"week_end"`
	Days    map[string]routeWaitDayBlock `json:"days"` // "weekday" / "weekend"
}

// generateAllRouteWaitTimeStats derives the passenger wait-time
// distribution (inspection-paradox transform of the observed headway
// distribution) for every route over the week, split by weekday vs
// weekend, and writes one JSON per route alongside the existing route
// stop-level stats.
//
// The histogram math: for a renewal process with observed headways
// {h_i}, a passenger arriving uniformly at random sees a gap of length
// h with probability proportional to h, then waits Uniform(0, h). So
// the wait-time mass landing in bin [a, b) is
//
//	mass(a, b) = Σ_i max(0, min(b, h_i) − a)
//
// and dividing by Σ_i h_i gives the density. Two BQ queries cover
// everything:
//
//  1. per (route, day_type)              — overall headway summary
//  2. per (route, day_type, hour, bin)   — wait-time histogram mass
//
// The hourly cells aggregate to the overall by summing across hours,
// and medians are derived in Go from the density (cumulative crosses
// 0.5, linearly interpolated within the crossing bin).
func generateAllRouteWaitTimeStats(ctx context.Context, weekStart, weekEnd civil.Date) error {
	summaries, err := queryRouteWaitSummary(ctx, weekStart, weekEnd)
	if err != nil {
		return fmt.Errorf("wait summary: %w", err)
	}
	hourlyHist, err := queryRouteWaitHourlyHistogram(ctx, weekStart, weekEnd)
	if err != nil {
		return fmt.Errorf("wait hourly histogram: %w", err)
	}

	byRoute := make(map[string]*routeWaitWeeklyStats)
	getRoute := func(rid string) *routeWaitWeeklyStats {
		r, ok := byRoute[rid]
		if !ok {
			r = &routeWaitWeeklyStats{
				RouteID: rid,
				WeekEnd: weekEnd.String(),
				Days:    make(map[string]routeWaitDayBlock),
			}
			byRoute[rid] = r
		}
		return r
	}

	for k, s := range summaries {
		r := getRoute(k.RouteID)
		block := r.Days[k.DayType]
		block.Summary = s
		r.Days[k.DayType] = block
	}

	// Aggregate hourly mass into overall mass per (route, day_type),
	// then compute densities + medians for both the overall block and
	// each hourly cell.
	for k, hours := range hourlyHist {
		r := getRoute(k.RouteID)
		block := r.Days[k.DayType]

		overall := make([]float64, waitHistBins)
		hourCells := make([]routeWaitHourCell, 0, len(hours))
		// Sort hour keys for stable per-hour output.
		hourList := make([]int, 0, len(hours))
		for h := range hours {
			hourList = append(hourList, h)
		}
		sort.Ints(hourList)
		for _, h := range hourList {
			cell := hours[h]
			for i, m := range cell.Mass {
				overall[i] += m
			}
			if cell.N < minHeadwaysForHourCell {
				continue
			}
			hist := densityFromMass(cell.Mass)
			median, _ := medianFromDensity(hist)
			mean := closedFormMeanWaitFromMass(cell.Mass)
			medianV := median
			cellOut := routeWaitHourCell{
				Hour:          h,
				N:             cell.N,
				MedianWaitMin: &medianV,
			}
			if cell.P50Headway > 0 {
				p50 := round1(cell.P50Headway)
				cellOut.P50HeadwayMin = &p50
			}
			if mean > 0 {
				v := round1(mean)
				cellOut.MeanWaitMin = &v
			}
			if v, ok := percentileFromDensity(hist, 0.95); ok {
				v = round1(v)
				cellOut.P95WaitMin = &v
			}
			if v, ok := percentileFromDensity(hist, 0.99); ok {
				v = round1(v)
				cellOut.P99WaitMin = &v
			}
			hourCells = append(hourCells, cellOut)
		}
		block.ByHour = hourCells

		block.Histogram = densityFromMass(overall)
		if median, ok := medianFromDensity(block.Histogram); ok {
			v := median
			block.Summary.MedianWaitMin = &v
		}
		if v, ok := percentileFromDensity(block.Histogram, 0.95); ok {
			v = round1(v)
			block.Summary.P95WaitMin = &v
		}
		if v, ok := percentileFromDensity(block.Histogram, 0.99); ok {
			v = round1(v)
			block.Summary.P99WaitMin = &v
		}
		r.Days[k.DayType] = block
	}

	weekEndStr := weekEnd.String()
	written := 0
	for _, r := range byRoute {
		payload, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			slog.Warn("route wait time marshal failed", "route_id", r.RouteID, "err", err)
			continue
		}
		key := fmt.Sprintf("%s%s/%s.json", routeWaitWeeklyPrefix, weekEndStr, sanitizeRouteID(r.RouteID))
		if err := writeObject(ctx, key, payload); err != nil {
			slog.Warn("route wait time write failed", "route_id", r.RouteID, "key", key, "err", err)
			continue
		}
		written++
	}
	slog.Info("route wait time weekly stats written", "week_end", weekEndStr, "routes", written)
	return nil
}

// headwaysCTE builds the SQL CTE chain ending in `headways(route_id,
// day_type, hour, h_min)`. day_type is "weekend" for Sun/Sat and
// "weekday" otherwise. hour is the PT hour-of-day of the leading
// arrival in the pair.
func headwaysCTE(weekStart, weekEnd civil.Date) string {
	return fmt.Sprintf(`%s,
arrivals AS (
  SELECT
    route_id, stop_id, service_date, actual_arrival,
    LEAD(actual_arrival) OVER (
      PARTITION BY route_id, stop_id, service_date
      ORDER BY actual_arrival
    ) AS next_arrival
  FROM obs
  WHERE actual_arrival IS NOT NULL AND is_stale = FALSE
),
headways AS (
  SELECT
    route_id,
    IF(EXTRACT(DAYOFWEEK FROM service_date) IN (1, 7), 'weekend', 'weekday') AS day_type,
    EXTRACT(HOUR FROM actual_arrival AT TIME ZONE 'America/Los_Angeles') AS hour,
    TIMESTAMP_DIFF(next_arrival, actual_arrival, SECOND) / 60.0 AS h_min
  FROM arrivals
  WHERE next_arrival IS NOT NULL
    AND TIMESTAMP_DIFF(next_arrival, actual_arrival, SECOND) BETWEEN %d AND %d
)`, dedupedRangeObservationsCTE(weekStart, weekEnd), waitMinHeadwaySeconds, waitMaxHeadwaySeconds)
}

type routeDayKey struct {
	RouteID string
	DayType string
}

func queryRouteWaitSummary(ctx context.Context, weekStart, weekEnd civil.Date) (map[routeDayKey]routeWaitSummary, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT
		  route_id,
		  day_type,
		  COUNT(*) AS n,
		  AVG(h_min) AS mean_h_min,
		  APPROX_QUANTILES(h_min, 100)[OFFSET(50)] AS p50_h_min,
		  SAFE_DIVIDE(SUM(POW(h_min, 2)), 2.0 * SUM(h_min)) AS mean_wait_min
		FROM headways
		GROUP BY route_id, day_type
	`, headwaysCTE(weekStart, weekEnd)))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[routeDayKey]routeWaitSummary)
	for {
		var row struct {
			RouteID     string               `bigquery:"route_id"`
			DayType     string               `bigquery:"day_type"`
			N           int64                `bigquery:"n"`
			MeanHMin    bigquery.NullFloat64 `bigquery:"mean_h_min"`
			P50HMin     bigquery.NullFloat64 `bigquery:"p50_h_min"`
			MeanWaitMin bigquery.NullFloat64 `bigquery:"mean_wait_min"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		s := routeWaitSummary{N: row.N}
		if row.MeanHMin.Valid {
			v := round1(row.MeanHMin.Float64)
			s.MeanHeadwayMin = &v
		}
		if row.P50HMin.Valid {
			v := round1(row.P50HMin.Float64)
			s.P50HeadwayMin = &v
		}
		if row.MeanWaitMin.Valid {
			v := round1(row.MeanWaitMin.Float64)
			s.MeanWaitMin = &v
		}
		out[routeDayKey{RouteID: row.RouteID, DayType: row.DayType}] = s
	}
	return out, nil
}

type routeHourMass struct {
	N          int64
	P50Headway float64
	Mass       []float64
}

// queryRouteWaitHourlyHistogram returns per (route, day_type, hour)
// the wait-time mass per 1-min bin (length waitHistBins), the headway
// count, and the median headway. Aggregating these hourly cells gives
// the overall (route, day_type) histogram for free.
func queryRouteWaitHourlyHistogram(ctx context.Context, weekStart, weekEnd civil.Date) (map[routeDayKey]map[int]*routeHourMass, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s,
		bins AS (
		  SELECT bin_lo
		  FROM UNNEST(GENERATE_ARRAY(0, %d)) AS bin_lo
		),
		hourly_stats AS (
		  SELECT
		    route_id, day_type, hour,
		    COUNT(*) AS n,
		    APPROX_QUANTILES(h_min, 100)[OFFSET(50)] AS p50_h_min
		  FROM headways
		  GROUP BY route_id, day_type, hour
		),
		hourly_mass AS (
		  SELECT
		    h.route_id, h.day_type, h.hour, b.bin_lo,
		    SUM(GREATEST(0.0, LEAST(b.bin_lo + 1.0, h.h_min) - b.bin_lo)) AS mass
		  FROM headways h
		  CROSS JOIN bins b
		  GROUP BY h.route_id, h.day_type, h.hour, b.bin_lo
		)
		SELECT
		  m.route_id,
		  m.day_type,
		  m.hour,
		  s.n,
		  s.p50_h_min,
		  m.bin_lo,
		  m.mass
		FROM hourly_mass m
		JOIN hourly_stats s
		  ON s.route_id = m.route_id AND s.day_type = m.day_type AND s.hour = m.hour
		ORDER BY route_id, day_type, hour, bin_lo
	`, headwaysCTE(weekStart, weekEnd), waitHistBins-1))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[routeDayKey]map[int]*routeHourMass)
	for {
		var row struct {
			RouteID string               `bigquery:"route_id"`
			DayType string               `bigquery:"day_type"`
			Hour    int64                `bigquery:"hour"`
			N       int64                `bigquery:"n"`
			P50HMin bigquery.NullFloat64 `bigquery:"p50_h_min"`
			BinLo   int64                `bigquery:"bin_lo"`
			Mass    bigquery.NullFloat64 `bigquery:"mass"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		k := routeDayKey{RouteID: row.RouteID, DayType: row.DayType}
		if _, ok := out[k]; !ok {
			out[k] = make(map[int]*routeHourMass)
		}
		cell, ok := out[k][int(row.Hour)]
		if !ok {
			cell = &routeHourMass{
				N:    row.N,
				Mass: make([]float64, waitHistBins),
			}
			if row.P50HMin.Valid {
				cell.P50Headway = row.P50HMin.Float64
			}
			out[k][int(row.Hour)] = cell
		}
		if row.BinLo >= 0 && int(row.BinLo) < waitHistBins {
			cell.Mass[row.BinLo] = row.Mass.Float64
		}
	}
	return out, nil
}

// densityFromMass normalizes the raw bin-mass slice (each entry is the
// total wait-time mass from observed headways falling into that bin)
// into a density that integrates to 1.0 across all bins. Empty input
// returns zero-density bins (the frontend treats that as "no data").
func densityFromMass(mass []float64) routeWaitHistogram {
	binLo := make([]int, len(mass))
	for i := range binLo {
		binLo[i] = i
	}
	total := 0.0
	for _, v := range mass {
		total += v
	}
	density := make([]float64, len(mass))
	if total > 0 {
		for i, v := range mass {
			density[i] = round3(v / total)
		}
	}
	return routeWaitHistogram{BinLoMin: binLo, Density: density}
}

// percentileFromDensity returns the wait-time percentile (in minutes)
// given a unit-width binned density, by walking the cumulative sum
// until it crosses p (0..1) and linearly interpolating within the
// crossing bin. Returns (0, false) when the density is empty/degenerate.
func percentileFromDensity(h routeWaitHistogram, p float64) (float64, bool) {
	totalDensity := 0.0
	for _, d := range h.Density {
		totalDensity += d
	}
	if totalDensity == 0 {
		return 0, false
	}
	cum := 0.0
	for i, d := range h.Density {
		next := cum + d
		if next >= p {
			if d <= 0 {
				return float64(i), true
			}
			frac := (p - cum) / d
			return round1(float64(i) + frac), true
		}
		cum = next
	}
	return float64(len(h.Density) - 1), true
}

// medianFromDensity is a thin wrapper around percentileFromDensity at p=0.5.
func medianFromDensity(h routeWaitHistogram) (float64, bool) {
	return percentileFromDensity(h, 0.5)
}

// closedFormMeanWaitFromMass computes E[H^2]/(2 E[H]) from raw
// bin-mass values. Bin i represents the wait mass in [i, i+1) min
// from headways whose contribution is GREATEST(0, LEAST(i+1, h) - i).
// Total mass per bin = Σ_h (contribution), which over all bins equals
// Σ h_i. The second moment of headway requires the actual h_i values,
// which we don't keep — but the closed-form mean wait can equivalently
// be expressed in terms of the density:
//
//	mean_wait = ∫ w · f_W(w) dw  ≈ Σ_i (i + 0.5) · density_i
//
// So we compute it from the binned density rather than from BQ-side
// aggregation, since this function is called per-hour where the BQ
// helper didn't run the SAFE_DIVIDE form.
func closedFormMeanWaitFromMass(mass []float64) float64 {
	total := 0.0
	weighted := 0.0
	for i, m := range mass {
		total += m
		weighted += (float64(i) + 0.5) * m
	}
	if total <= 0 {
		return 0
	}
	return weighted / total
}

func round3(f float64) float64 {
	return math.Round(f*1000) / 1000
}

// binMassFromHeadways is the pure-Go equivalent of the BQ
// `SUM(GREATEST(0, LEAST(b+1, h) - b))` aggregation used in
// queryRouteWaitHourlyHistogram. Kept here so the inspection-paradox
// math can be unit-tested without a live BQ connection. Bin width is
// 1 min; binCount is typically waitHistBins (90).
func binMassFromHeadways(headways []float64, binCount int) []float64 {
	out := make([]float64, binCount)
	for _, h := range headways {
		if h <= 0 {
			continue
		}
		// Each bin [i, i+1) receives max(0, min(i+1, h) - i).
		// Bins with i >= h contribute nothing. Bins fully covered
		// (i+1 <= h) contribute 1. The straddling bin contributes
		// (h - floor(h)).
		full := int(h)
		if full > binCount {
			full = binCount
		}
		for i := 0; i < full; i++ {
			out[i] += 1.0
		}
		if full < binCount {
			frac := h - float64(full)
			if frac > 0 {
				out[full] += frac
			}
		}
	}
	return out
}
