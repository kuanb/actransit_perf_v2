package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	statsLatestKey      = "stats/latest.json"
	statsArchivePrefix  = "stats/"
)

type dailyStats struct {
	ServiceDate        string             `json:"service_date"`
	GeneratedAt        time.Time          `json:"generated_at"`
	System             systemStats        `json:"system"`
	ScheduleCompliance scheduleCompliance `json:"schedule_compliance"`
	DelayHistogram     delayHistogram     `json:"delay_histogram"`
	Routes             []routeStats       `json:"routes"`
}

type systemStats struct {
	TotalTrips        int64   `json:"total_trips"`
	TotalObservations int64   `json:"total_observations"`
	VehiclesObserved  int64   `json:"vehicles_observed"`
	OnTimePct         float64 `json:"on_time_pct"`
	Within5MinPct     float64 `json:"within_5min_pct"`
	Within7MinPct     float64 `json:"within_7min_pct"`
	LatePct           float64 `json:"late_pct"`
	EarlyPct          float64 `json:"early_pct"`
	P50DelayMinutes   float64 `json:"p50_delay_minutes"`
	P95DelayMinutes   float64 `json:"p95_delay_minutes"`
	AvgSpeedMph       float64 `json:"avg_speed_mph"`
}

type scheduleCompliance struct {
	ScheduledTrips        int      `json:"scheduled_trips"`
	RanTrips              int      `json:"ran_trips"`
	DroppedTrips          int      `json:"dropped_trips"`
	DroppedTripIDsSample  []string `json:"dropped_trip_ids_sample"`
}

type delayHistogram struct {
	Buckets []string `json:"buckets"`
	Labels  []string `json:"labels"`
	Counts  []int64  `json:"counts"`
}

type routeStats struct {
	RouteID             string   `json:"route_id"`
	TripsObserved       int64    `json:"trips_observed"`
	Observations        int64    `json:"observations"`
	OnTimePct           float64  `json:"on_time_pct"`
	Within5MinPct       float64  `json:"within_5min_pct"`
	Within7MinPct       float64  `json:"within_7min_pct"`
	LatePct             float64  `json:"late_pct"`
	EarlyPct            float64  `json:"early_pct"`
	P5DelayMinutes      *float64 `json:"p5_delay_minutes"`
	P25DelayMinutes     *float64 `json:"p25_delay_minutes"`
	P50DelayMinutes     *float64 `json:"p50_delay_minutes"`
	P75DelayMinutes     *float64 `json:"p75_delay_minutes"`
	P95DelayMinutes     *float64 `json:"p95_delay_minutes"`
	AvgSpeedMph         float64  `json:"avg_speed_mph"`
	Color               string   `json:"color"`
	TextColor           string   `json:"text_color"`
	ScheduledTrips      int      `json:"scheduled_trips"`
	RanTrips            int      `json:"ran_trips"`
	ServiceDeliveredPct *float64 `json:"service_delivered_pct"`
}

type bqStatsStats struct {
	Stats *systemStats
}

// generateDailyStats runs the full pipeline: download GTFS, query BigQuery,
// combine, write the JSON to GCS at stats/<date>.json AND stats/latest.json.
// Returns the rendered struct (also for the HTTP response body).
func generateDailyStats(ctx context.Context, serviceDate civil.Date) (*dailyStats, error) {
	gtfsBytes, err := readGTFSCurrentZip(ctx)
	if err != nil {
		return nil, fmt.Errorf("read gtfs zip: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(gtfsBytes), int64(len(gtfsBytes)))
	if err != nil {
		return nil, fmt.Errorf("open gtfs zip: %w", err)
	}

	activeServices, err := loadActiveServices(zr, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("active services: %w", err)
	}
	scheduledTripRoute, err := loadScheduledTripRoutes(zr, activeServices)
	if err != nil {
		return nil, fmt.Errorf("scheduled trips: %w", err)
	}
	colors, err := loadRouteColors(zr)
	if err != nil {
		return nil, fmt.Errorf("route colors: %w", err)
	}

	scheduledByRoute := make(map[string]int)
	for _, rid := range scheduledTripRoute {
		scheduledByRoute[rid]++
	}

	ranTrips, err := queryRanTrips(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query ran trips: %w", err)
	}

	ranByRoute := make(map[string]int)
	for tid := range ranTrips {
		if rid, ok := scheduledTripRoute[tid]; ok {
			ranByRoute[rid]++
		}
	}

	sys, err := queryStatsSystem(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query system stats: %w", err)
	}
	routes, err := queryStatsPerRoute(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query route stats: %w", err)
	}
	hist, err := queryStatsHistogram(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query histogram: %w", err)
	}

	for i := range routes {
		rid := routes[i].RouteID
		if c, ok := colors[rid]; ok {
			routes[i].Color = c.color
			routes[i].TextColor = c.text
		} else {
			routes[i].Color = "FFFFFF"
			routes[i].TextColor = "000000"
		}
		sched := scheduledByRoute[rid]
		ran := ranByRoute[rid]
		routes[i].ScheduledTrips = sched
		routes[i].RanTrips = ran
		if sched > 0 {
			pct := round1(100 * float64(ran) / float64(sched))
			routes[i].ServiceDeliveredPct = &pct
		}
	}

	scheduled := make(map[string]struct{}, len(scheduledTripRoute))
	for tid := range scheduledTripRoute {
		scheduled[tid] = struct{}{}
	}
	dropped := make([]string, 0)
	ranInSchedule := 0
	for tid := range scheduled {
		if _, ok := ranTrips[tid]; ok {
			ranInSchedule++
		} else {
			dropped = append(dropped, tid)
		}
	}
	sort.Strings(dropped)
	sample := dropped
	if len(sample) > 20 {
		sample = sample[:20]
	}

	out := &dailyStats{
		ServiceDate: serviceDate.String(),
		GeneratedAt: time.Now().UTC(),
		System:      *sys,
		ScheduleCompliance: scheduleCompliance{
			ScheduledTrips:       len(scheduled),
			RanTrips:             ranInSchedule,
			DroppedTrips:         len(dropped),
			DroppedTripIDsSample: sample,
		},
		DelayHistogram: hist,
		Routes:         routes,
	}

	payload, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	if err := writeObject(ctx, statsLatestKey, payload); err != nil {
		return nil, fmt.Errorf("write latest: %w", err)
	}
	archiveKey := fmt.Sprintf("%s%s.json", statsArchivePrefix, serviceDate.String())
	if err := writeObject(ctx, archiveKey, payload); err != nil {
		return nil, fmt.Errorf("write archive %s: %w", archiveKey, err)
	}
	if err := updateStatsIndex(ctx, serviceDate.String()); err != nil {
		return out, fmt.Errorf("update stats index: %w", err)
	}
	return out, nil
}

const statsIndexKey = "stats/_index.json"

type statsIndex struct {
	Dates     []string  `json:"dates"`
	UpdatedAt time.Time `json:"updated_at"`
}

// updateStatsIndex maintains stats/_index.json — a list of all archived
// dates, sorted descending. The frontend reads this to populate its
// "view another date" dropdown.
func updateStatsIndex(ctx context.Context, dateStr string) error {
	idx := statsIndex{}
	r, err := gcsClient.Bucket(bucketName).Object(statsIndexKey).NewReader(ctx)
	if err == nil {
		body, rerr := io.ReadAll(r)
		r.Close()
		if rerr == nil {
			_ = json.Unmarshal(body, &idx)
		}
	}
	for _, d := range idx.Dates {
		if d == dateStr {
			idx.UpdatedAt = time.Now().UTC()
			payload, _ := json.MarshalIndent(idx, "", "  ")
			return writeObject(ctx, statsIndexKey, payload)
		}
	}
	idx.Dates = append(idx.Dates, dateStr)
	sort.Sort(sort.Reverse(sort.StringSlice(idx.Dates)))
	idx.UpdatedAt = time.Now().UTC()
	payload, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return writeObject(ctx, statsIndexKey, payload)
}

func defaultStatsServiceDate(now time.Time) civil.Date {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	pt := now.In(loc)
	if pt.Hour() < 4 {
		pt = pt.Add(-24 * time.Hour)
	}
	return civil.DateOf(pt)
}

func readGTFSCurrentZip(ctx context.Context) ([]byte, error) {
	r, err := gcsClient.Bucket(bucketName).Object(gtfsCurrentKey).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func openZipCSV(zr *zip.Reader, name string) (*csv.Reader, io.Closer, []string, error) {
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, nil, nil, err
		}
		cr := csv.NewReader(rc)
		cr.FieldsPerRecord = -1
		cr.TrimLeadingSpace = true
		cr.ReuseRecord = true
		headers, err := cr.Read()
		if err != nil {
			rc.Close()
			return nil, nil, nil, err
		}
		if len(headers) > 0 {
			headers[0] = strings.TrimPrefix(headers[0], "\ufeff")
		}
		return cr, rc, headers, nil
	}
	return nil, nil, nil, fmt.Errorf("missing %s in zip", name)
}

func headerIndex(headers []string) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return idx
}

func col(row []string, idx map[string]int, name string) string {
	i, ok := idx[name]
	if !ok || i >= len(row) {
		return ""
	}
	return row[i]
}

// loadActiveServices applies the GTFS calendar.txt + calendar_dates.txt logic
// for a target date. Pure helper exposed for testing via in-memory zip.
func loadActiveServices(zr *zip.Reader, target civil.Date) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	yyyymmdd := fmt.Sprintf("%04d%02d%02d", target.Year, target.Month, target.Day)
	weekday := strings.ToLower(time.Date(target.Year, target.Month, target.Day, 0, 0, 0, 0, time.UTC).Weekday().String())

	cr, rc, headers, err := openZipCSV(zr, "calendar.txt")
	if err == nil {
		idx := headerIndex(headers)
		for {
			row, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				rc.Close()
				return nil, err
			}
			start := col(row, idx, "start_date")
			end := col(row, idx, "end_date")
			if start <= yyyymmdd && yyyymmdd <= end {
				if col(row, idx, weekday) == "1" {
					out[col(row, idx, "service_id")] = struct{}{}
				}
			}
		}
		rc.Close()
	}

	cr, rc, headers, err = openZipCSV(zr, "calendar_dates.txt")
	if err == nil {
		idx := headerIndex(headers)
		for {
			row, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				rc.Close()
				return nil, err
			}
			if col(row, idx, "date") != yyyymmdd {
				continue
			}
			sid := col(row, idx, "service_id")
			switch col(row, idx, "exception_type") {
			case "1":
				out[sid] = struct{}{}
			case "2":
				delete(out, sid)
			}
		}
		rc.Close()
	}

	return out, nil
}

func loadScheduledTripRoutes(zr *zip.Reader, services map[string]struct{}) (map[string]string, error) {
	cr, rc, headers, err := openZipCSV(zr, "trips.txt")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	idx := headerIndex(headers)
	out := make(map[string]string)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if _, ok := services[col(row, idx, "service_id")]; !ok {
			continue
		}
		out[col(row, idx, "trip_id")] = col(row, idx, "route_id")
	}
	return out, nil
}

type colorPair struct {
	color string
	text  string
}

func loadRouteColors(zr *zip.Reader) (map[string]colorPair, error) {
	cr, rc, headers, err := openZipCSV(zr, "routes.txt")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	idx := headerIndex(headers)
	out := make(map[string]colorPair)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		c := strings.ToUpper(strings.TrimSpace(col(row, idx, "route_color")))
		if c == "" {
			c = "FFFFFF"
		}
		t := strings.ToUpper(strings.TrimSpace(col(row, idx, "route_text_color")))
		if t == "" {
			t = "000000"
		}
		out[col(row, idx, "route_id")] = colorPair{color: c, text: t}
	}
	return out, nil
}

func queryRanTrips(ctx context.Context, serviceDate civil.Date) (map[string]struct{}, error) {
	q := bqClient.Query(fmt.Sprintf(`
		SELECT DISTINCT trip_id
		FROM `+"`%s.actransit.trip_observations`"+`
		WHERE service_date = "%s"
		  AND actual_arrival IS NOT NULL
	`, projectID, serviceDate))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for {
		var row struct {
			TripID string `bigquery:"trip_id"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out[row.TripID] = struct{}{}
	}
	return out, nil
}

func queryStatsSystem(ctx context.Context, serviceDate civil.Date) (*systemStats, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH base AS (
		  SELECT trip_id, vehicle_id, delay_seconds, leg_avg_speed_mps
		  FROM `+"`%s.actransit.trip_observations`"+`
		  WHERE service_date = "%s"
		    AND actual_arrival IS NOT NULL
		    AND is_stale = FALSE
		)
		SELECT
		  COUNT(DISTINCT trip_id) AS total_trips,
		  COUNT(*) AS total_observations,
		  COUNT(DISTINCT vehicle_id) AS vehicles_observed,
		  ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 180, 1, 0)) * 100, 1) AS on_time_pct,
		  ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 300, 1, 0)) * 100, 1) AS within_5min_pct,
		  ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 420, 1, 0)) * 100, 1) AS within_7min_pct,
		  ROUND(AVG(IF(delay_seconds < 0, 1, 0)) * 100, 1) AS early_pct,
		  ROUND(AVG(IF(delay_seconds > 180, 1, 0)) * 100, 1) AS late_pct,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_delay_seconds,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_delay_seconds,
		  ROUND(AVG(leg_avg_speed_mps) * 2.2369, 1) AS avg_speed_mph
		FROM base
	`, projectID, serviceDate))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var row struct {
		TotalTrips        bigquery.NullInt64   `bigquery:"total_trips"`
		TotalObservations bigquery.NullInt64   `bigquery:"total_observations"`
		VehiclesObserved  bigquery.NullInt64   `bigquery:"vehicles_observed"`
		OnTimePct         bigquery.NullFloat64 `bigquery:"on_time_pct"`
		Within5MinPct     bigquery.NullFloat64 `bigquery:"within_5min_pct"`
		Within7MinPct     bigquery.NullFloat64 `bigquery:"within_7min_pct"`
		EarlyPct          bigquery.NullFloat64 `bigquery:"early_pct"`
		LatePct           bigquery.NullFloat64 `bigquery:"late_pct"`
		P50Sec            bigquery.NullInt64   `bigquery:"p50_delay_seconds"`
		P95Sec            bigquery.NullInt64   `bigquery:"p95_delay_seconds"`
		AvgSpeedMph       bigquery.NullFloat64 `bigquery:"avg_speed_mph"`
	}
	if err := it.Next(&row); err != nil {
		if err == iterator.Done {
			return &systemStats{}, nil
		}
		return nil, err
	}
	return &systemStats{
		TotalTrips:        row.TotalTrips.Int64,
		TotalObservations: row.TotalObservations.Int64,
		VehiclesObserved:  row.VehiclesObserved.Int64,
		OnTimePct:         row.OnTimePct.Float64,
		Within5MinPct:     row.Within5MinPct.Float64,
		Within7MinPct:     row.Within7MinPct.Float64,
		EarlyPct:          row.EarlyPct.Float64,
		LatePct:           row.LatePct.Float64,
		P50DelayMinutes:   round1(float64(row.P50Sec.Int64) / 60.0),
		P95DelayMinutes:   round1(float64(row.P95Sec.Int64) / 60.0),
		AvgSpeedMph:       row.AvgSpeedMph.Float64,
	}, nil
}

func queryStatsPerRoute(ctx context.Context, serviceDate civil.Date) ([]routeStats, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH base AS (
		  SELECT route_id, trip_id, delay_seconds, leg_avg_speed_mps
		  FROM `+"`%s.actransit.trip_observations`"+`
		  WHERE service_date = "%s"
		    AND actual_arrival IS NOT NULL
		    AND is_stale = FALSE
		)
		SELECT
		  route_id,
		  COUNT(DISTINCT trip_id) AS trips_observed,
		  COUNT(*) AS observations,
		  ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 180, 1, 0)) * 100, 1) AS on_time_pct,
		  ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 300, 1, 0)) * 100, 1) AS within_5min_pct,
		  ROUND(AVG(IF(delay_seconds BETWEEN 0 AND 420, 1, 0)) * 100, 1) AS within_7min_pct,
		  ROUND(AVG(IF(delay_seconds < 0, 1, 0)) * 100, 1) AS early_pct,
		  ROUND(AVG(IF(delay_seconds > 180, 1, 0)) * 100, 1) AS late_pct,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(5)] AS p5_delay_seconds,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(25)] AS p25_delay_seconds,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_delay_seconds,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(75)] AS p75_delay_seconds,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_delay_seconds,
		  ROUND(AVG(leg_avg_speed_mps) * 2.2369, 1) AS avg_speed_mph
		FROM base
		GROUP BY route_id
		ORDER BY trips_observed DESC, observations DESC
	`, projectID, serviceDate))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []routeStats
	for {
		var row struct {
			RouteID       string               `bigquery:"route_id"`
			TripsObserved bigquery.NullInt64   `bigquery:"trips_observed"`
			Observations  bigquery.NullInt64   `bigquery:"observations"`
			OnTimePct     bigquery.NullFloat64 `bigquery:"on_time_pct"`
			Within5MinPct bigquery.NullFloat64 `bigquery:"within_5min_pct"`
			Within7MinPct bigquery.NullFloat64 `bigquery:"within_7min_pct"`
			EarlyPct      bigquery.NullFloat64 `bigquery:"early_pct"`
			LatePct       bigquery.NullFloat64 `bigquery:"late_pct"`
			P5Sec         bigquery.NullInt64   `bigquery:"p5_delay_seconds"`
			P25Sec        bigquery.NullInt64   `bigquery:"p25_delay_seconds"`
			P50Sec        bigquery.NullInt64   `bigquery:"p50_delay_seconds"`
			P75Sec        bigquery.NullInt64   `bigquery:"p75_delay_seconds"`
			P95Sec        bigquery.NullInt64   `bigquery:"p95_delay_seconds"`
			AvgSpeedMph   bigquery.NullFloat64 `bigquery:"avg_speed_mph"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, routeStats{
			RouteID:         row.RouteID,
			TripsObserved:   row.TripsObserved.Int64,
			Observations:    row.Observations.Int64,
			OnTimePct:       row.OnTimePct.Float64,
			Within5MinPct:   row.Within5MinPct.Float64,
			Within7MinPct:   row.Within7MinPct.Float64,
			EarlyPct:        row.EarlyPct.Float64,
			LatePct:         row.LatePct.Float64,
			P5DelayMinutes:  nullableMinutes(row.P5Sec),
			P25DelayMinutes: nullableMinutes(row.P25Sec),
			P50DelayMinutes: nullableMinutes(row.P50Sec),
			P75DelayMinutes: nullableMinutes(row.P75Sec),
			P95DelayMinutes: nullableMinutes(row.P95Sec),
			AvgSpeedMph:     row.AvgSpeedMph.Float64,
		})
	}
	return out, nil
}

func queryStatsHistogram(ctx context.Context, serviceDate civil.Date) (delayHistogram, error) {
	bucketOrder := []string{"very_early", "early", "on_time", "slightly_late", "late", "very_late"}
	labels := []string{"< -2 min", "-2 to -1 min", "-1 to +1 min", "+1 to +3 min", "+3 to +10 min", "> +10 min"}
	q := bqClient.Query(fmt.Sprintf(`
		SELECT
		  CASE
		    WHEN delay_seconds < -120 THEN 'very_early'
		    WHEN delay_seconds < -60  THEN 'early'
		    WHEN delay_seconds <= 60  THEN 'on_time'
		    WHEN delay_seconds <= 180 THEN 'slightly_late'
		    WHEN delay_seconds <= 600 THEN 'late'
		    ELSE                            'very_late'
		  END AS bucket,
		  COUNT(*) AS n
		FROM `+"`%s.actransit.trip_observations`"+`
		WHERE service_date = "%s"
		  AND actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY bucket
	`, projectID, serviceDate))
	it, err := q.Read(ctx)
	if err != nil {
		return delayHistogram{}, err
	}
	counts := make(map[string]int64)
	for {
		var row struct {
			Bucket string `bigquery:"bucket"`
			N      int64  `bigquery:"n"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return delayHistogram{}, err
		}
		counts[row.Bucket] = row.N
	}
	final := make([]int64, len(bucketOrder))
	for i, b := range bucketOrder {
		final[i] = counts[b]
	}
	return delayHistogram{Buckets: bucketOrder, Labels: labels, Counts: final}, nil
}

func nullableMinutes(v bigquery.NullInt64) *float64 {
	if !v.Valid {
		return nil
	}
	m := round1(float64(v.Int64) / 60.0)
	return &m
}

func round1(f float64) float64 {
	return float64(int64(f*10+sign(f)*0.5)) / 10.0
}

func sign(f float64) float64 {
	if f < 0 {
		return -1
	}
	return 1
}

// Avoid unused-import lint when this file is built without callers wiring it up.
var _ = strconv.Itoa
