package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"

	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	routeSpeedWeeklyPrefix = "stats/weekly/route_speed/"

	// 0.5 mph bins covering 0..70 mph. Bin i represents the half-open
	// interval [i*speedBinMph, (i+1)*speedBinMph) mph. Bus leg speeds
	// almost always sit below ~50 mph; the upper tail captures the rare
	// projection glitches without losing resolution near the mode.
	speedBinMph   = 0.5
	speedBinCount = 140

	// Drop per-hour cells with fewer than this many legs to avoid wild
	// percentile noise from undersampled hours (e.g. 04:00 on Sunday).
	// Mirrors minHeadwaysForHourCell in route_wait_time.go.
	minLegsForHourCell = 10

	// Mph per m/s; matches the constant used elsewhere in stats.go.
	mpsToMph = 2.2369
)

type routeSpeedSummary struct {
	N         int64    `json:"n"`
	MeanMph   *float64 `json:"mean_mph"`
	P5Mph     *float64 `json:"p5_mph"`
	P50Mph    *float64 `json:"p50_mph"`
	P95Mph    *float64 `json:"p95_mph"`
	P99Mph    *float64 `json:"p99_mph"`
	StddevMph *float64 `json:"stddev_mph"`
}

type routeSpeedHourCell struct {
	Hour      int      `json:"hour"`
	N         int64    `json:"n"`
	MeanMph   *float64 `json:"mean_mph"`
	P5Mph     *float64 `json:"p5_mph"`
	P50Mph    *float64 `json:"p50_mph"`
	P95Mph    *float64 `json:"p95_mph"`
	P99Mph    *float64 `json:"p99_mph"`
	StddevMph *float64 `json:"stddev_mph"`
}

type routeSpeedDayBlock struct {
	Summary routeSpeedSummary    `json:"summary"`
	ByHour  []routeSpeedHourCell `json:"by_hour"`
}

// routeSegmentSpeedSummary describes the speed distribution for one
// (from_stop, to_stop) leg over the whole week, regardless of hour or
// day-of-week. The frontend route map uses these to color each segment
// of the route shape between two consecutive stops.
type routeSegmentSpeedSummary struct {
	FromStopID string   `json:"from_stop_id"`
	ToStopID   string   `json:"to_stop_id"`
	N          int64    `json:"n"`
	MeanMph    *float64 `json:"mean_mph"`
	P50Mph     *float64 `json:"p50_mph"`
	StddevMph  *float64 `json:"stddev_mph"`
}

type routeSpeedDirectionBlock struct {
	Days     map[string]routeSpeedDayBlock `json:"days"` // "weekday" / "weekend"
	Segments []routeSegmentSpeedSummary    `json:"segments,omitempty"`
}

type routeSpeedWeeklyStats struct {
	RouteID    string                              `json:"route_id"`
	WeekEnd    string                              `json:"week_end"`
	Directions map[string]routeSpeedDirectionBlock `json:"directions"` // "0" / "1"
}

// generateAllRouteSpeedStats aggregates per-leg average speeds from
// trip_observations over the week, grouped by (route_id, direction_id,
// day_type, hour), and writes one JSON file per route alongside the
// existing route_wait and route_stops outputs.
//
// trip_observations doesn't carry direction_id, so the BQ query emits
// per-trip histograms and Go-side routing maps each trip_id to its
// GTFS direction via the in-process gtfsCache. Trips not present in
// the cache (deprecated trip IDs from earlier feeds) are skipped.
func generateAllRouteSpeedStats(ctx context.Context, weekStart, weekEnd civil.Date) error {
	cache := ensureGTFSCache(ctx)
	if cache == nil {
		return fmt.Errorf("gtfs cache is nil; /refresh-gtfs has not yet processed the feed")
	}

	rows, err := queryRouteSpeedTripHistograms(ctx, weekStart, weekEnd)
	if err != nil {
		return fmt.Errorf("speed histograms: %w", err)
	}

	byRoute, skippedTripMisses := aggregateRouteSpeedRows(rows, cache)

	segRows, err := queryRouteSegmentSpeedHistograms(ctx, weekStart, weekEnd)
	if err != nil {
		return fmt.Errorf("segment speed histograms: %w", err)
	}
	segByRoute, skippedSegmentMisses := aggregateRouteSegmentSpeedRows(segRows, cache)

	weekEndStr := weekEnd.String()
	written := 0
	for routeID, byDir := range byRoute {
		out := routeSpeedWeeklyStats{
			RouteID:    routeID,
			WeekEnd:    weekEndStr,
			Directions: make(map[string]routeSpeedDirectionBlock),
		}
		for dirID, byDayType := range byDir {
			block := routeSpeedDirectionBlock{Days: make(map[string]routeSpeedDayBlock)}
			for dayType, byHour := range byDayType {
				block.Days[dayType] = buildSpeedDayBlock(byHour)
			}
			if segDirs, ok := segByRoute[routeID]; ok {
				if pairs, ok := segDirs[dirID]; ok {
					block.Segments = buildSegmentSummaries(pairs)
				}
			}
			out.Directions[strconv.Itoa(dirID)] = block
		}
		payload, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			slog.Warn("route speed marshal failed", "route_id", routeID, "err", err)
			continue
		}
		key := fmt.Sprintf("%s%s/%s.json", routeSpeedWeeklyPrefix, weekEndStr, sanitizeRouteID(routeID))
		if err := writeObject(ctx, key, payload); err != nil {
			slog.Warn("route speed write failed", "route_id", routeID, "key", key, "err", err)
			continue
		}
		written++
	}
	slog.Info("route speed weekly stats written",
		"week_end", weekEndStr,
		"routes", written,
		"skipped_trip_cache_misses", skippedTripMisses,
		"skipped_segment_cache_misses", skippedSegmentMisses,
	)
	return nil
}

// aggregateRouteSpeedRows partitions histogram rows by route + direction
// (looked up from the gtfsCache), day_type, and hour, and merges per-row
// bin counts into the per-cell []int64. Rows whose trip_id isn't in the
// cache are skipped; the returned int counts those skips for logging.
// The shape is map[route_id]map[direction_id]map[day_type]map[hour]bins.
func aggregateRouteSpeedRows(rows []routeSpeedHistRow, cache *gtfsCache) (map[string]map[int]map[string]map[int][]int64, int) {
	byRoute := make(map[string]map[int]map[string]map[int][]int64)
	getCell := func(routeID string, dirID int, dayType string, hour int) []int64 {
		r, ok := byRoute[routeID]
		if !ok {
			r = make(map[int]map[string]map[int][]int64)
			byRoute[routeID] = r
		}
		d, ok := r[dirID]
		if !ok {
			d = make(map[string]map[int][]int64)
			r[dirID] = d
		}
		dt, ok := d[dayType]
		if !ok {
			dt = make(map[int][]int64)
			d[dayType] = dt
		}
		bins, ok := dt[hour]
		if !ok {
			bins = make([]int64, speedBinCount)
			dt[hour] = bins
		}
		return bins
	}

	skipped := 0
	for _, row := range rows {
		route, ok := cache.Routes[row.RouteID]
		if !ok {
			skipped++
			continue
		}
		trip, ok := route.Trips[row.TripID]
		if !ok {
			skipped++
			continue
		}
		if row.Bin < 0 || int(row.Bin) >= speedBinCount {
			continue
		}
		bins := getCell(row.RouteID, trip.DirectionID, row.DayType, int(row.Hour))
		bins[row.Bin] += row.N
	}
	return byRoute, skipped
}

type routeSpeedHistRow struct {
	RouteID string `bigquery:"route_id"`
	TripID  string `bigquery:"trip_id"`
	DayType string `bigquery:"day_type"`
	Hour    int64  `bigquery:"hour"`
	Bin     int64  `bigquery:"bin"`
	N       int64  `bigquery:"n"`
}

// queryRouteSpeedTripHistograms emits per-(route_id, trip_id, day_type,
// hour, speed_bin) leg counts for the week. The bin index is computed
// in SQL so the wire format stays compact (one row per non-empty bin).
// leg_avg_speed_mps is capped at 35 m/s (~78 mph) to drop the rare
// projection-glitch rows without losing real distribution tail mass.
func queryRouteSpeedTripHistograms(ctx context.Context, weekStart, weekEnd civil.Date) ([]routeSpeedHistRow, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s,
		legs AS (
		  SELECT
		    route_id,
		    trip_id,
		    IF(EXTRACT(DAYOFWEEK FROM service_date) IN (1, 7), 'weekend', 'weekday') AS day_type,
		    EXTRACT(HOUR FROM actual_arrival AT TIME ZONE 'America/Los_Angeles') AS hour,
		    CAST(FLOOR(leg_avg_speed_mps * %f / %f) AS INT64) AS bin
		  FROM obs
		  WHERE actual_arrival IS NOT NULL
		    AND is_stale = FALSE
		    AND leg_avg_speed_mps IS NOT NULL
		    AND leg_avg_speed_mps BETWEEN 0 AND 35
		)
		SELECT route_id, trip_id, day_type, hour, bin, COUNT(*) AS n
		FROM legs
		GROUP BY route_id, trip_id, day_type, hour, bin
	`, dedupedRangeObservationsCTE(weekStart, weekEnd), mpsToMph, speedBinMph))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []routeSpeedHistRow
	for {
		var row routeSpeedHistRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

// buildSpeedDayBlock takes the per-hour bin map for a single (route,
// direction, day_type) and produces the JSON-shaped day block: an
// overall summary (sum across hours) plus the per-hour cells (cells
// below minLegsForHourCell are dropped).
func buildSpeedDayBlock(byHour map[int][]int64) routeSpeedDayBlock {
	hours := make([]int, 0, len(byHour))
	for h := range byHour {
		hours = append(hours, h)
	}
	sort.Ints(hours)

	overall := make([]int64, speedBinCount)
	cells := make([]routeSpeedHourCell, 0, len(hours))
	for _, h := range hours {
		bins := byHour[h]
		var n int64
		for i, c := range bins {
			overall[i] += c
			n += c
		}
		if n < minLegsForHourCell {
			continue
		}
		cells = append(cells, summarizeSpeedHistogramCell(h, bins))
	}

	return routeSpeedDayBlock{
		Summary: summarizeSpeedHistogramSummary(overall),
		ByHour:  cells,
	}
}

func summarizeSpeedHistogramCell(hour int, bins []int64) routeSpeedHourCell {
	s := summarizeSpeedHistogramSummary(bins)
	return routeSpeedHourCell{
		Hour:      hour,
		N:         s.N,
		MeanMph:   s.MeanMph,
		P5Mph:     s.P5Mph,
		P50Mph:    s.P50Mph,
		P95Mph:    s.P95Mph,
		P99Mph:    s.P99Mph,
		StddevMph: s.StddevMph,
	}
}

// summarizeSpeedHistogramSummary computes mean, p50/p95/p99, and stddev
// (all in mph) from a count-per-bin slice. Bin i covers
// [i*speedBinMph, (i+1)*speedBinMph) mph; the midpoint approximation
// is used for both the mean and the variance, which is accurate to
// well within the half-bin width.
func summarizeSpeedHistogramSummary(bins []int64) routeSpeedSummary {
	var n int64
	var sum, sumSq float64
	for i, c := range bins {
		if c == 0 {
			continue
		}
		mid := (float64(i) + 0.5) * speedBinMph
		n += c
		sum += mid * float64(c)
		sumSq += mid * mid * float64(c)
	}
	out := routeSpeedSummary{N: n}
	if n == 0 {
		return out
	}
	mean := sum / float64(n)
	meanR := round1(mean)
	out.MeanMph = &meanR
	if n > 1 {
		// Population variance (N denominator) — fine for descriptive
		// stats over a fixed week of observations; the difference vs
		// sample variance is negligible at these counts.
		variance := sumSq/float64(n) - mean*mean
		if variance < 0 {
			variance = 0
		}
		stddev := round1(math.Sqrt(variance))
		out.StddevMph = &stddev
	}
	if v, ok := speedPercentileFromBins(bins, n, 0.05); ok {
		v = round1(v)
		out.P5Mph = &v
	}
	if v, ok := speedPercentileFromBins(bins, n, 0.50); ok {
		v = round1(v)
		out.P50Mph = &v
	}
	if v, ok := speedPercentileFromBins(bins, n, 0.95); ok {
		v = round1(v)
		out.P95Mph = &v
	}
	if v, ok := speedPercentileFromBins(bins, n, 0.99); ok {
		v = round1(v)
		out.P99Mph = &v
	}
	return out
}

type routeSegmentSpeedHistRow struct {
	RouteID      string `bigquery:"route_id"`
	TripID       string `bigquery:"trip_id"`
	StopSequence int64  `bigquery:"stop_sequence"`
	Bin          int64  `bigquery:"bin"`
	N            int64  `bigquery:"n"`
}

// segmentPairKey identifies one (from_stop, to_stop) leg within a
// route+direction. Two trips can produce the same pair when their
// stop_times share consecutive stop_ids, so segment counts merge
// across trips at this granularity.
type segmentPairKey struct {
	FromStopID string
	ToStopID   string
}

// queryRouteSegmentSpeedHistograms emits per-(route_id, trip_id,
// stop_sequence, speed_bin) leg counts for the week with no day_type
// or hour breakdown. The stop_sequence identifies which leg of the
// trip the count belongs to; Go-side resolution maps that to a
// (from_stop, to_stop) pair via the cached GTFS-static stop_times.
// leg_avg_speed_mps cap mirrors the per-hour query.
func queryRouteSegmentSpeedHistograms(ctx context.Context, weekStart, weekEnd civil.Date) ([]routeSegmentSpeedHistRow, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s,
		legs AS (
		  SELECT
		    route_id,
		    trip_id,
		    stop_sequence,
		    CAST(FLOOR(leg_avg_speed_mps * %f / %f) AS INT64) AS bin
		  FROM obs
		  WHERE actual_arrival IS NOT NULL
		    AND is_stale = FALSE
		    AND leg_avg_speed_mps IS NOT NULL
		    AND leg_avg_speed_mps BETWEEN 0 AND 35
		)
		SELECT route_id, trip_id, stop_sequence, bin, COUNT(*) AS n
		FROM legs
		GROUP BY route_id, trip_id, stop_sequence, bin
	`, dedupedRangeObservationsCTE(weekStart, weekEnd), mpsToMph, speedBinMph))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []routeSegmentSpeedHistRow
	for {
		var row routeSegmentSpeedHistRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

// aggregateRouteSegmentSpeedRows resolves each (route, trip, stop_seq)
// row into a (route, direction, from_stop_id, to_stop_id) bucket using
// the GTFS cache, then merges bin counts within each bucket. Rows
// whose trip isn't in the cache or whose stop_sequence has no
// predecessor (first stop in the trip — no leg) are silently dropped;
// the returned int counts trip-lookup misses for logging.
func aggregateRouteSegmentSpeedRows(rows []routeSegmentSpeedHistRow, cache *gtfsCache) (map[string]map[int]map[segmentPairKey][]int64, int) {
	byRoute := make(map[string]map[int]map[segmentPairKey][]int64)
	getCell := func(routeID string, dirID int, key segmentPairKey) []int64 {
		r, ok := byRoute[routeID]
		if !ok {
			r = make(map[int]map[segmentPairKey][]int64)
			byRoute[routeID] = r
		}
		d, ok := r[dirID]
		if !ok {
			d = make(map[segmentPairKey][]int64)
			r[dirID] = d
		}
		bins, ok := d[key]
		if !ok {
			bins = make([]int64, speedBinCount)
			d[key] = bins
		}
		return bins
	}

	skipped := 0
	for _, row := range rows {
		route, ok := cache.Routes[row.RouteID]
		if !ok {
			skipped++
			continue
		}
		trip, ok := route.Trips[row.TripID]
		if !ok {
			skipped++
			continue
		}
		pair, ok := lookupStopPair(trip, int(row.StopSequence))
		if !ok {
			continue
		}
		if row.Bin < 0 || int(row.Bin) >= speedBinCount {
			continue
		}
		bins := getCell(row.RouteID, trip.DirectionID, pair)
		bins[row.Bin] += row.N
	}
	return byRoute, skipped
}

// lookupStopPair finds the leg in trip.StopTimes that arrives at the
// given stop_sequence, returning (prev_stop_id, this_stop_id). Returns
// !ok when the sequence is the first stop in the trip (no predecessor)
// or isn't present at all. stop_times is already sorted by
// stop_sequence in streamStopTimes; we walk linearly because trips
// are short (typically <80 stops).
func lookupStopPair(trip gtfsTrip, stopSeq int) (segmentPairKey, bool) {
	for i, st := range trip.StopTimes {
		if st.StopSequence != stopSeq {
			continue
		}
		if i == 0 {
			return segmentPairKey{}, false
		}
		return segmentPairKey{FromStopID: trip.StopTimes[i-1].StopID, ToStopID: st.StopID}, true
	}
	return segmentPairKey{}, false
}

// buildSegmentSummaries computes the per-pair summary (mean / p50 /
// stddev) for one (route, direction) and sorts deterministically so
// the JSON output is stable across regenerations.
func buildSegmentSummaries(pairs map[segmentPairKey][]int64) []routeSegmentSpeedSummary {
	keys := make([]segmentPairKey, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].FromStopID != keys[j].FromStopID {
			return keys[i].FromStopID < keys[j].FromStopID
		}
		return keys[i].ToStopID < keys[j].ToStopID
	})
	out := make([]routeSegmentSpeedSummary, 0, len(keys))
	for _, k := range keys {
		s := summarizeSpeedHistogramSummary(pairs[k])
		if s.N == 0 {
			continue
		}
		out = append(out, routeSegmentSpeedSummary{
			FromStopID: k.FromStopID,
			ToStopID:   k.ToStopID,
			N:          s.N,
			MeanMph:    s.MeanMph,
			P50Mph:     s.P50Mph,
			StddevMph:  s.StddevMph,
		})
	}
	return out
}

// speedPercentileFromBins returns the p-quantile (in mph) by walking
// the cumulative count until it crosses p*total and linearly
// interpolating within the crossing bin. n must equal the sum of
// bins. Returns (0, false) on empty input.
func speedPercentileFromBins(bins []int64, n int64, p float64) (float64, bool) {
	if n <= 0 {
		return 0, false
	}
	target := p * float64(n)
	var cum float64
	for i, c := range bins {
		if c == 0 {
			continue
		}
		next := cum + float64(c)
		if next >= target {
			frac := (target - cum) / float64(c)
			if frac < 0 {
				frac = 0
			}
			if frac > 1 {
				frac = 1
			}
			return (float64(i) + frac) * speedBinMph, true
		}
		cum = next
	}
	return float64(len(bins)) * speedBinMph, true
}
