package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	statsLatestKey                  = "stats/latest.json"
	statsArchivePrefix              = "stats/"
	limitedRouteScheduledRunsPerDay = 10
)

type dailyStats struct {
	MethodologyVersion   int                 `json:"methodology_version"`
	ServiceDate          string              `json:"service_date"`
	GeneratedAt          time.Time           `json:"generated_at"`
	System               systemStats         `json:"system"`
	ScheduleCompliance   scheduleCompliance  `json:"schedule_compliance"`
	DistortionHistogram  distortionHistogram `json:"distortion_histogram"`
	DelayMinuteHistogram []minuteBucket      `json:"delay_minute_histogram"`
	VolumeHistogram      volumeHistogram     `json:"volume_15min_histogram"`
	Routes               []routeStats        `json:"routes"`
}

// volumeHistogram: 96 fifteen-minute buckets covering one PT day (00:00 →
// 23:45), counting observed stop arrivals per bucket. Routes is the top-N
// non-limited routes by daily volume plus a synthetic "Other" series.
// Totals is the across-included-routes sum per bucket, independent of the
// top-N reduction so the y-axis reflects the plotted population.
type volumeHistogram struct {
	Routes []volumeRouteSeries `json:"routes"`
	Totals []int64             `json:"totals"`
}

type volumeRouteSeries struct {
	RouteID   string  `json:"route_id"`
	Color     string  `json:"color"`
	TextColor string  `json:"text_color"`
	Counts    []int64 `json:"counts"`
}

type distortionHistogram struct {
	Buckets []string `json:"buckets"`
	Counts  []int64  `json:"counts"`
}

type minuteBucket struct {
	Minute int   `json:"minute"`
	Count  int64 `json:"count"`
}

type systemStats struct {
	TotalTrips        int64          `json:"total_trips"`
	TotalObservations int64          `json:"total_observations"`
	VehiclesObserved  int64          `json:"vehicles_observed"`
	OnTimePct         float64        `json:"on_time_pct"`
	Within5MinPct     float64        `json:"within_5min_pct"`
	Within7MinPct     float64        `json:"within_7min_pct"`
	LatePct           float64        `json:"late_pct"`
	EarlyPct          float64        `json:"early_pct"`
	P50DelayMinutes   float64        `json:"p50_delay_minutes"`
	P95DelayMinutes   float64        `json:"p95_delay_minutes"`
	AvgSpeedMph       float64        `json:"avg_speed_mph"`
	Bunching          *bunchingStats `json:"bunching,omitempty"`
}

type scheduleCompliance struct {
	ScheduledTrips                int                       `json:"scheduled_trips"`
	RanTrips                      int                       `json:"ran_trips"`
	DroppedTrips                  int                       `json:"dropped_trips"`
	TripsNotCompleted             int                       `json:"trips_not_completed"`
	TripsNotCompletedDistribution *notCompletedDistribution `json:"trips_not_completed_distribution,omitempty"`
	DroppedTripIDsSample          []string                  `json:"dropped_trip_ids_sample"`
	// Stop-level service delivery excludes origins, final terminals, and
	// no-pickup stops. The denominator comes from GTFS, including dropped trips.
	StopSDPct        *float64 `json:"stop_sd_pct,omitempty"`
	StopSDN          int64    `json:"stop_sd_n,omitempty"`
	StopSDDeliveredN int64    `json:"stop_sd_delivered_n,omitempty"`
}

// notCompletedDistribution describes how far trips counted in
// TripsNotCompleted got relative to their last passenger-boarding stop.
type notCompletedDistribution struct {
	P5Pct  float64 `json:"p5_pct"`
	P25Pct float64 `json:"p25_pct"`
	P50Pct float64 `json:"p50_pct"`
	P75Pct float64 `json:"p75_pct"`
	P95Pct float64 `json:"p95_pct"`
	// 10 buckets, each a 10% slice: index 0 = [0, 10), …, index 9 = [90, 100).
	Histogram []int64 `json:"histogram"`
}

type routeStats struct {
	RouteID          string         `json:"route_id"`
	TripsObserved    int64          `json:"trips_observed"`
	Observations     int64          `json:"observations"`
	OnTimePct        float64        `json:"on_time_pct"`
	Within5MinPct    float64        `json:"within_5min_pct"`
	Within7MinPct    float64        `json:"within_7min_pct"`
	LatePct          float64        `json:"late_pct"`
	EarlyPct         float64        `json:"early_pct"`
	P5DelayMinutes   *float64       `json:"p5_delay_minutes"`
	P25DelayMinutes  *float64       `json:"p25_delay_minutes"`
	P50DelayMinutes  *float64       `json:"p50_delay_minutes"`
	P75DelayMinutes  *float64       `json:"p75_delay_minutes"`
	P95DelayMinutes  *float64       `json:"p95_delay_minutes"`
	P50DistortionPct *float64       `json:"p50_distortion_pct"`
	P95DistortionPct *float64       `json:"p95_distortion_pct"`
	AvgSpeedMph      float64        `json:"avg_speed_mph"`
	Color            string         `json:"color"`
	TextColor        string         `json:"text_color"`
	ScheduledTrips   int            `json:"scheduled_trips"`
	RanTrips         int            `json:"ran_trips"`
	Limited          bool           `json:"limited"`
	TripDeliveryPct  *float64       `json:"trip_delivery_pct"`
	StopSDPct        *float64       `json:"stop_sd_pct,omitempty"`
	StopSDN          int64          `json:"stop_sd_n,omitempty"`
	StopSDDeliveredN int64          `json:"stop_sd_delivered_n,omitempty"`
	TwoBusGapWindows *int           `json:"two_bus_gap_windows,omitempty"`
	Bunching         *bunchingStats `json:"bunching,omitempty"`
}

type bqStatsStats struct {
	Stats *systemStats
}

// generateDailyStats runs the full pipeline: download GTFS, query BigQuery,
// combine, write the JSON to GCS at stats/<date>.json AND stats/latest.json.
// Returns the rendered struct (also for the HTTP response body).
func generateDailyStats(ctx context.Context, serviceDate civil.Date) (*dailyStats, error) {
	gtfsBytes, err := readGTFSZipForServiceDate(ctx, serviceDate)
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
	stopPlan, err := loadScheduledStopPlan(zr, scheduledTripRoute)
	if err != nil {
		return nil, fmt.Errorf("scheduled stops: %w", err)
	}
	colors, err := loadRouteColors(zr)
	if err != nil {
		return nil, fmt.Errorf("route colors: %w", err)
	}

	scheduledByRoute := make(map[string]int)
	for _, rid := range scheduledTripRoute {
		scheduledByRoute[rid]++
	}

	tripProgress, err := queryTripProgress(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query trip progress: %w", err)
	}
	ranTrips := make(map[string]struct{}, len(tripProgress))
	for tripID := range tripProgress {
		ranTrips[tripID] = struct{}{}
	}
	scheduledRuns, err := loadScheduledRuns(zr, serviceDate, activeServices)
	if err != nil {
		return nil, fmt.Errorf("load scheduled runs: %w", err)
	}
	twoBusGapWindowsByRoute := countTwoBusGapWindows(scheduledRuns, ranTrips)
	notCompleted, notCompletedDist := computeTripsNotCompleted(tripProgress, stopPlan)

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
	// queryStatsSystem filters to is_stale = FALSE so the per-stop delay
	// stats (on-time %, p50/p95, etc.) are computed from clean data. But
	// COUNT(DISTINCT trip_id) under that filter excludes trips whose only
	// rows are is_stale=TRUE (mostly residue from the pre-v16 parked-bus
	// thrash bug). For the headline trip count we want the full set so the
	// page agrees with "Observed running" in schedule compliance.
	sys.TotalTrips = int64(len(ranTrips))
	routes, err := queryStatsPerRoute(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query route stats: %w", err)
	}
	deliveredStops, err := queryDeliveredStops(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query delivered stops: %w", err)
	}
	routeStopSD := computeRouteStopSD(stopPlan, deliveredStops)
	minuteHist, err := queryStatsMinuteHistogram(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query minute histogram: %w", err)
	}
	volumeByRoute, err := queryStatsVolume15Min(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query volume 15min histogram: %w", err)
	}

	scheduleByStop, err := loadScheduleByStop(zr, serviceDate, activeServices)
	if err != nil {
		return nil, fmt.Errorf("load schedule by stop: %w", err)
	}
	observations, err := queryStatsObservations(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("query observations: %w", err)
	}
	distHist, distByRoute := computeDistortion(observations, scheduleByStop)
	systemBunching, routeBunching, err := calculateDailyBunching(ctx, zr, serviceDate, activeServices)
	if err != nil {
		return nil, fmt.Errorf("calculate bunching: %w", err)
	}
	sys.Bunching = systemBunching

	// queryStatsPerRoute only returns routes that had ≥1 observation row in
	// BigQuery for the day, so routes that were 100% dropped (or whose
	// observations weren't synthesized — e.g. trip_id missing from the GTFS
	// cache) silently disappear from the table. Backfill the missing rows
	// from scheduledByRoute so a fully-dropped route shows up with zeros
	// instead of vanishing — that's the only way the per-route view stays
	// consistent with system aggregate "service delivered %".
	seenInBQ := make(map[string]struct{}, len(routes))
	for _, r := range routes {
		seenInBQ[r.RouteID] = struct{}{}
	}
	for rid := range scheduledByRoute {
		if _, ok := seenInBQ[rid]; ok {
			continue
		}
		routes = append(routes, routeStats{RouteID: rid})
	}

	var sysStopTotalN, sysStopDeliveredN int64
	for i := range routes {
		rid := routes[i].RouteID
		routes[i].Bunching = routeBunching[rid]
		if routes[i].Bunching == nil {
			routes[i].Bunching = finalizeBunchingStats(bunchingAccumulator{}, nil, nil, time.Now().UTC(), 1, 1)
		}
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
		routes[i].Limited = isLimitedRoute(rid, float64(sched))
		if sched > 0 {
			twoBusGapWindows := twoBusGapWindowsByRoute[rid]
			routes[i].TwoBusGapWindows = &twoBusGapWindows
			pct := round1(100 * float64(ran) / float64(sched))
			routes[i].TripDeliveryPct = &pct
		}
		if pt, ok := routeStopSD[rid]; ok {
			pct := pt.Pct
			routes[i].StopSDPct = &pct
			routes[i].StopSDN = pt.TotalN
			routes[i].StopSDDeliveredN = pt.DeliveredN
			sysStopTotalN += pt.TotalN
			sysStopDeliveredN += pt.DeliveredN
		}
		if vals, ok := distByRoute[rid]; ok && len(vals) > 0 {
			sort.Float64s(vals)
			p50 := round1(percentileSorted(vals, 0.50))
			p95 := round1(percentileSorted(vals, 0.95))
			routes[i].P50DistortionPct = &p50
			routes[i].P95DistortionPct = &p95
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

	sc := scheduleCompliance{
		ScheduledTrips:                len(scheduled),
		RanTrips:                      ranInSchedule,
		DroppedTrips:                  len(dropped),
		TripsNotCompleted:             notCompleted,
		TripsNotCompletedDistribution: notCompletedDist,
		DroppedTripIDsSample:          sample,
		StopSDN:                       sysStopTotalN,
		StopSDDeliveredN:              sysStopDeliveredN,
	}
	if sysStopTotalN > 0 {
		pct := round1(100.0 * float64(sysStopDeliveredN) / float64(sysStopTotalN))
		sc.StopSDPct = &pct
	}

	out := &dailyStats{
		MethodologyVersion:   3,
		ServiceDate:          serviceDate.String(),
		GeneratedAt:          time.Now().UTC(),
		System:               *sys,
		ScheduleCompliance:   sc,
		DistortionHistogram:  distHist,
		DelayMinuteHistogram: minuteHist,
		VolumeHistogram:      buildVolumeHistogram(volumeByRoute, colors, scheduledByRoute, 10),
		Routes:               routes,
	}

	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	// stats/latest.json should always reflect the newest service_date.
	// generateDailyStats is called by both the daily cron AND backfill;
	// blindly overwriting would let an older-date backfill clobber it.
	// Skip the latest write when we'd be replacing newer data.
	updateLatest, err := isAtLeastAsRecentAsLatest(ctx, serviceDate)
	if err != nil {
		slog.Warn("compare to existing latest failed; writing anyway", "err", err)
		updateLatest = true
	}
	if updateLatest {
		if err := writeObject(ctx, statsLatestKey, payload); err != nil {
			return nil, fmt.Errorf("write latest: %w", err)
		}
	} else {
		slog.Info("generate-daily-stats skipped latest write",
			"service_date", serviceDate.String(),
			"reason", "older than current stats/latest.json",
		)
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

// isAtLeastAsRecentAsLatest returns true when candidate is the same as,
// or newer than, the service_date in the existing stats/latest.json. Used
// to gate latest.json overwrites so a backfill of an older date doesn't
// clobber newer data. Missing or malformed existing latest counts as
// "go ahead and write" — first-run case is the obvious one.
func isAtLeastAsRecentAsLatest(ctx context.Context, candidate civil.Date) (bool, error) {
	body, exists, err := readObject(ctx, statsLatestKey)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	var existing dailyStats
	if err := json.Unmarshal(body, &existing); err != nil {
		return false, fmt.Errorf("parse existing latest: %w", err)
	}
	if existing.ServiceDate == "" {
		return true, nil
	}
	existingDate, err := civil.ParseDate(existing.ServiceDate)
	if err != nil {
		return false, fmt.Errorf("parse existing service_date %q: %w", existing.ServiceDate, err)
	}
	return !candidate.Before(existingDate), nil
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

func readGTFSZipForServiceDate(ctx context.Context, serviceDate civil.Date) ([]byte, error) {
	current, err := readGTFSCurrentZip(ctx)
	if err != nil {
		return nil, err
	}
	supported, err := gtfsSupportsServiceDate(current, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", gtfsCurrentKey, err)
	}
	if supported {
		return current, nil
	}

	it := gcsClient.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: "gtfs/"})
	var archiveKeys []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list gtfs archives: %w", err)
		}
		if attrs.Name != gtfsCurrentKey && strings.HasSuffix(attrs.Name, ".zip") {
			archiveKeys = append(archiveKeys, attrs.Name)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(archiveKeys)))

	for _, key := range archiveKeys {
		body, exists, err := readObject(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", key, err)
		}
		if !exists {
			continue
		}
		supported, err := gtfsSupportsServiceDate(body, serviceDate)
		if err != nil {
			return nil, fmt.Errorf("inspect %s: %w", key, err)
		}
		if supported {
			slog.Info("using archived gtfs", "service_date", serviceDate.String(), "key", key)
			return body, nil
		}
	}
	return nil, fmt.Errorf("no current or archived GTFS feed covers %s", serviceDate)
}

func gtfsSupportsServiceDate(body []byte, serviceDate civil.Date) (bool, error) {
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return false, err
	}
	services, err := loadActiveServices(zr, serviceDate)
	if err != nil {
		return false, err
	}
	return len(services) > 0, nil
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

func loadScheduledStopPlan(zr *zip.Reader, tripRoutes map[string]string) (*scheduledStopPlan, error) {
	type scheduledStop struct {
		sequence      int64
		pickupAllowed bool
	}

	cr, rc, headers, err := openZipCSV(zr, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	idx := headerIndex(headers)
	byTrip := make(map[string][]scheduledStop)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tripID := col(row, idx, "trip_id")
		if _, ok := tripRoutes[tripID]; !ok {
			continue
		}
		sequence, err := strconv.ParseInt(col(row, idx, "stop_sequence"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("trip %s has invalid stop_sequence: %w", tripID, err)
		}
		byTrip[tripID] = append(byTrip[tripID], scheduledStop{
			sequence:      sequence,
			pickupAllowed: col(row, idx, "pickup_type") != "1",
		})
	}

	plan := &scheduledStopPlan{
		ScoredStops:          make(map[scheduledStopKey]string),
		ScoredByRoute:        make(map[string]int64),
		StopPositions:        make(map[scheduledStopKey]int),
		LastBoardingSequence: make(map[string]int64),
		LastBoardingPosition: make(map[string]int),
	}
	for tripID, stops := range byTrip {
		sort.Slice(stops, func(i, j int) bool { return stops[i].sequence < stops[j].sequence })
		routeID := tripRoutes[tripID]
		for i, stop := range stops {
			key := scheduledStopKey{TripID: tripID, StopSequence: stop.sequence}
			position := i + 1
			plan.StopPositions[key] = position
			if stop.pickupAllowed && i < len(stops)-1 {
				plan.LastBoardingSequence[tripID] = stop.sequence
				plan.LastBoardingPosition[tripID] = position
			}
			if i == 0 || i == len(stops)-1 || !stop.pickupAllowed {
				continue
			}
			plan.ScoredStops[key] = routeID
			plan.ScoredByRoute[routeID]++
		}
	}
	return plan, nil
}

type scheduledRun struct {
	TripID      string
	RouteID     string
	DirectionID string
	Start       time.Time
}

func loadScheduledRuns(zr *zip.Reader, serviceDate civil.Date, services map[string]struct{}) ([]scheduledRun, error) {
	cr, rc, headers, err := openZipCSV(zr, "trips.txt")
	if err != nil {
		return nil, err
	}
	idx := headerIndex(headers)
	tripMeta := make(map[string]scheduledRun)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			rc.Close()
			return nil, err
		}
		if _, ok := services[col(row, idx, "service_id")]; !ok {
			continue
		}
		tripID := col(row, idx, "trip_id")
		tripMeta[tripID] = scheduledRun{
			TripID:      tripID,
			RouteID:     col(row, idx, "route_id"),
			DirectionID: col(row, idx, "direction_id"),
		}
	}
	rc.Close()

	cr, rc, headers, err = openZipCSV(zr, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	idx = headerIndex(headers)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tripID := col(row, idx, "trip_id")
		run, ok := tripMeta[tripID]
		if !ok {
			continue
		}
		start := parseScheduledArrival(serviceDate, col(row, idx, "arrival_time"))
		if start.IsZero() {
			start = parseScheduledArrival(serviceDate, col(row, idx, "departure_time"))
		}
		if start.IsZero() || (!run.Start.IsZero() && !start.Before(run.Start)) {
			continue
		}
		run.Start = start
		tripMeta[tripID] = run
	}

	runs := make([]scheduledRun, 0, len(tripMeta))
	for _, run := range tripMeta {
		if !run.Start.IsZero() {
			runs = append(runs, run)
		}
	}
	return runs, nil
}

func countTwoBusGapWindows(runs []scheduledRun, observedTrips map[string]struct{}) map[string]int {
	type routeDirection struct {
		RouteID     string
		DirectionID string
	}
	byRouteDirection := make(map[routeDirection][]scheduledRun)
	for _, run := range runs {
		key := routeDirection{RouteID: run.RouteID, DirectionID: run.DirectionID}
		byRouteDirection[key] = append(byRouteDirection[key], run)
	}

	out := make(map[string]int)
	for key, group := range byRouteDirection {
		sort.Slice(group, func(i, j int) bool {
			if group[i].Start.Equal(group[j].Start) {
				return group[i].TripID < group[j].TripID
			}
			return group[i].Start.Before(group[j].Start)
		})
		for i := 1; i < len(group); i++ {
			_, previousObserved := observedTrips[group[i-1].TripID]
			_, currentObserved := observedTrips[group[i].TripID]
			if !previousObserved && !currentObserved {
				out[key.RouteID]++
			}
		}
	}
	return out
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

func queryDeliveredStops(ctx context.Context, serviceDate civil.Date) ([]scheduledStopKey, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT trip_id, stop_sequence
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND delay_seconds BETWEEN -60 AND 420
	`, dedupedDayObservationsCTE(serviceDate)))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []scheduledStopKey
	for {
		var row scheduledStopKey
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

func computeRouteStopSD(plan *scheduledStopPlan, delivered []scheduledStopKey) map[string]routeStopSDPoint {
	out := make(map[string]routeStopSDPoint, len(plan.ScoredByRoute))
	for routeID, total := range plan.ScoredByRoute {
		out[routeID] = routeStopSDPoint{TotalN: total}
	}
	for _, stop := range delivered {
		routeID, ok := plan.ScoredStops[stop]
		if !ok {
			continue
		}
		point := out[routeID]
		point.DeliveredN++
		out[routeID] = point
	}
	for routeID, point := range out {
		if point.TotalN > 0 {
			point.Pct = round1(100 * float64(point.DeliveredN) / float64(point.TotalN))
			out[routeID] = point
		}
	}
	return out
}

func queryTripProgress(ctx context.Context, serviceDate civil.Date) (map[string]int64, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT trip_id, MAX(stop_sequence) AS last_observed_sequence
		FROM obs
		WHERE actual_arrival IS NOT NULL
		GROUP BY trip_id
	`, dedupedDayObservationsCTE(serviceDate)))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	for {
		var row struct {
			TripID               string `bigquery:"trip_id"`
			LastObservedSequence int64  `bigquery:"last_observed_sequence"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out[row.TripID] = row.LastObservedSequence
	}
	return out, nil
}

func computeTripsNotCompleted(progress map[string]int64, plan *scheduledStopPlan) (int, *notCompletedDistribution) {
	var completionPcts []float64
	hist := make([]int64, 10)
	for tripID, lastObservedSequence := range progress {
		lastBoardingSequence, ok := plan.LastBoardingSequence[tripID]
		if !ok || lastObservedSequence >= lastBoardingSequence {
			continue
		}
		lastPosition, ok := plan.StopPositions[scheduledStopKey{TripID: tripID, StopSequence: lastObservedSequence}]
		lastBoardingPosition := plan.LastBoardingPosition[tripID]
		if !ok || lastBoardingPosition == 0 {
			continue
		}
		completionPct := 100 * float64(lastPosition) / float64(lastBoardingPosition)
		completionPcts = append(completionPcts, completionPct)
		bucket := min(int(completionPct/10), 9)
		hist[bucket]++
	}
	if len(completionPcts) == 0 {
		return 0, nil
	}
	sort.Float64s(completionPcts)
	return len(completionPcts), &notCompletedDistribution{
		P5Pct:     round1(percentileSorted(completionPcts, 0.05)),
		P25Pct:    round1(percentileSorted(completionPcts, 0.25)),
		P50Pct:    round1(percentileSorted(completionPcts, 0.50)),
		P75Pct:    round1(percentileSorted(completionPcts, 0.75)),
		P95Pct:    round1(percentileSorted(completionPcts, 0.95)),
		Histogram: hist,
	}
}

// dedupedDayObservationsCTE renders a CTE definition named `obs` that
// selects rows from trip_observations for the given service_date,
// reduced to one row per (trip_id, stop_sequence). Live tracking can
// finalize the same trip many times per minute (root cause under
// investigation — see AGENTS.md "live duplication" footgun); without
// this dedup, every aggregate is inflated by the duplication factor.
// Backfilled days are unaffected because /backfill-day uses
// WriteTruncate. The ORDER BY prefers rows that have a non-null
// actual_arrival, since the post-completion "fresh re-incarnation" of
// a trip can otherwise overwrite real arrival data with empty rows.
func dedupedDayObservationsCTE(sd civil.Date) string {
	return fmt.Sprintf(`obs AS (
	  SELECT * FROM `+"`%s.actransit.trip_observations`"+`
	  WHERE service_date = "%s"
	  QUALIFY ROW_NUMBER() OVER (
	    PARTITION BY trip_id, stop_sequence
	    ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
	  ) = 1
	)`, projectID, sd)
}

// dedupedRangeObservationsCTE is the multi-day variant. Partitions
// include service_date so each day dedups independently.
func dedupedRangeObservationsCTE(start, end civil.Date) string {
	return fmt.Sprintf(`obs AS (
	  SELECT * FROM `+"`%s.actransit.trip_observations`"+`
	  WHERE service_date BETWEEN "%s" AND "%s"
	  QUALIFY ROW_NUMBER() OVER (
	    PARTITION BY service_date, trip_id, stop_sequence
	    ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
	  ) = 1
	)`, projectID, start, end)
}

func queryStatsSystem(ctx context.Context, serviceDate civil.Date) (*systemStats, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s,
		base AS (
		  SELECT trip_id, vehicle_id, delay_seconds, leg_avg_speed_mps
		  FROM obs
		  WHERE actual_arrival IS NOT NULL
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
	`, dedupedDayObservationsCTE(serviceDate)))
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
		WITH %s,
		base AS (
		  SELECT route_id, trip_id, delay_seconds, leg_avg_speed_mps
		  FROM obs
		  WHERE actual_arrival IS NOT NULL
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
	`, dedupedDayObservationsCTE(serviceDate)))
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

type stopKey struct {
	RouteID string
	StopID  string
}

type scheduledStopKey struct {
	TripID       string `bigquery:"trip_id"`
	StopSequence int64  `bigquery:"stop_sequence"`
}

type scheduledStopPlan struct {
	ScoredStops          map[scheduledStopKey]string
	ScoredByRoute        map[string]int64
	StopPositions        map[scheduledStopKey]int
	LastBoardingSequence map[string]int64
	LastBoardingPosition map[string]int
}

// loadScheduleByStop reads stop_times.txt + trips.txt and produces a sorted
// (ascending) list of scheduled UTC arrival timestamps per (route_id, stop_id)
// for the target service_date. Only trips with active service_id are included.
// This is the source of truth for headway calculations because it includes
// trips that were scheduled but dropped (no observation in BQ).
func loadScheduleByStop(zr *zip.Reader, serviceDate civil.Date, services map[string]struct{}) (map[stopKey][]time.Time, error) {
	tripCSV, tripRC, tripHeaders, err := openZipCSV(zr, "trips.txt")
	if err != nil {
		return nil, err
	}
	tripIdx := headerIndex(tripHeaders)
	tripToRoute := make(map[string]string)
	for {
		row, err := tripCSV.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			tripRC.Close()
			return nil, err
		}
		if _, ok := services[col(row, tripIdx, "service_id")]; !ok {
			continue
		}
		tripToRoute[col(row, tripIdx, "trip_id")] = col(row, tripIdx, "route_id")
	}
	tripRC.Close()

	stCSV, stRC, stHeaders, err := openZipCSV(zr, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	defer stRC.Close()
	stIdx := headerIndex(stHeaders)
	out := make(map[stopKey][]time.Time)
	for {
		row, err := stCSV.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tid := col(row, stIdx, "trip_id")
		rid, ok := tripToRoute[tid]
		if !ok {
			continue
		}
		ts := parseScheduledArrival(serviceDate, col(row, stIdx, "arrival_time"))
		if ts.IsZero() {
			continue
		}
		key := stopKey{RouteID: rid, StopID: col(row, stIdx, "stop_id")}
		out[key] = append(out[key], ts)
	}
	for k, v := range out {
		sort.Slice(v, func(i, j int) bool { return v[i].Before(v[j]) })
		out[k] = v
	}
	return out, nil
}

type observationRow struct {
	RouteID          string    `bigquery:"route_id"`
	StopID           string    `bigquery:"stop_id"`
	ScheduledArrival time.Time `bigquery:"scheduled_arrival"`
	DelaySeconds     int64     `bigquery:"delay_seconds"`
}

func queryStatsObservations(ctx context.Context, serviceDate civil.Date) ([]observationRow, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT route_id, stop_id, scheduled_arrival, delay_seconds
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND scheduled_arrival IS NOT NULL
		  AND is_stale = FALSE
	`, dedupedDayObservationsCTE(serviceDate)))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []observationRow
	for {
		var row observationRow
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

// distortionBucketLabels returns 42 labels covering [-100%, +100%] in 5%
// increments, plus extreme buckets at each end:
//
//	"≤ -100%", "-100% to -95%", "-95% to -90%", ..., "+95% to +100%", "≥ +100%"
//
// Index 0 is the under-flow bucket; index 41 is the over-flow bucket.
func distortionBucketLabels() []string {
	labels := make([]string, 0, 42)
	labels = append(labels, "≤ -100%")
	for lo := -100; lo < 100; lo += 5 {
		hi := lo + 5
		labels = append(labels, fmt.Sprintf("%+d%% to %+d%%", lo, hi))
	}
	labels = append(labels, "≥ +100%")
	return labels
}

// distortionBucketIndex maps a distortion percent to a histogram bucket index
// in the 42-bucket scheme described above.
func distortionBucketIndex(d float64) int {
	if d <= -100 {
		return 0
	}
	if d >= 100 {
		return 41
	}
	// 5% buckets across [-100, +100); shift so -100 → 0, then int-divide by 5
	return 1 + int((d+100)/5)
}

// computeDistortion calculates per-observation headway distortion as a
// percent of the relevant scheduled headway:
//   - late  (delay > 0): distortion = +delay / (this_sched − prior_sched) * 100
//   - early (delay < 0): distortion = +delay / (next_sched − this_sched) * 100  (negative because delay is)
//
// Returns the system-wide histogram (42 buckets, 5% wide except for the two
// extreme catch-all buckets) and a per-route map of all distortion values
// (caller computes percentiles).
func computeDistortion(obs []observationRow, schedule map[stopKey][]time.Time) (distortionHistogram, map[string][]float64) {
	labels := distortionBucketLabels()
	counts := make([]int64, len(labels))
	byRoute := make(map[string][]float64)

	for _, o := range obs {
		if o.DelaySeconds == 0 {
			continue
		}
		sched, ok := schedule[stopKey{o.RouteID, o.StopID}]
		if !ok || len(sched) < 2 {
			continue
		}
		idx := sort.Search(len(sched), func(i int) bool { return !sched[i].Before(o.ScheduledArrival) })
		if idx >= len(sched) || !sched[idx].Equal(o.ScheduledArrival) {
			continue
		}
		var headway time.Duration
		if o.DelaySeconds > 0 {
			if idx == 0 {
				continue
			}
			headway = o.ScheduledArrival.Sub(sched[idx-1])
		} else {
			if idx+1 >= len(sched) {
				continue
			}
			headway = sched[idx+1].Sub(o.ScheduledArrival)
		}
		if headway <= 0 {
			continue
		}
		d := float64(o.DelaySeconds) / headway.Seconds() * 100.0
		byRoute[o.RouteID] = append(byRoute[o.RouteID], d)
		counts[distortionBucketIndex(d)]++
	}
	return distortionHistogram{Buckets: labels, Counts: counts}, byRoute
}

// percentileSorted returns the value at percentile p (0..1) from a sorted
// (ascending) slice. Uses nearest-rank.
func percentileSorted(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// queryStatsMinuteHistogram returns one bucket per (whole) minute of delay
// observed today. Buckets outside the [-15, +45] minute window are clamped
// to the boundary buckets to prevent extreme outliers from blowing up the
// JSON or the chart's x-axis.
func queryStatsMinuteHistogram(ctx context.Context, serviceDate civil.Date) ([]minuteBucket, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT
		  CASE
		    WHEN delay_seconds < -15*60 THEN -15
		    WHEN delay_seconds >  45*60 THEN  45
		    ELSE CAST(FLOOR(delay_seconds / 60.0) AS INT64)
		  END AS minute,
		  COUNT(*) AS n
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY minute
		ORDER BY minute
	`, dedupedDayObservationsCTE(serviceDate)))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []minuteBucket
	for {
		var row struct {
			Minute int64 `bigquery:"minute"`
			N      int64 `bigquery:"n"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, minuteBucket{Minute: int(row.Minute), Count: row.N})
	}
	return out, nil
}

// queryStatsVolume15Min counts observed stop arrivals per (route, 15-min PT
// window) for the given service_date. Bucket 0 = [00:00, 00:15) PT,
// bucket 95 = [23:45, 24:00). Owl runs whose actual_arrival lands after the
// next PT midnight (bucket ≥ 96) are dropped; the service_date filter in the
// CTE already restricts to one calendar partition, so the leakage is small.
func queryStatsVolume15Min(ctx context.Context, serviceDate civil.Date) (map[string][]int64, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT
		  route_id,
		  CAST(FLOOR(TIMESTAMP_DIFF(
		    actual_arrival,
		    TIMESTAMP(DATETIME("%s 00:00:00"), "America/Los_Angeles"),
		    MINUTE
		  ) / 15.0) AS INT64) AS bucket,
		  COUNT(*) AS n
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY route_id, bucket
		HAVING bucket BETWEEN 0 AND 95
	`, dedupedDayObservationsCTE(serviceDate), serviceDate))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]int64)
	for {
		var row struct {
			RouteID string `bigquery:"route_id"`
			Bucket  int64  `bigquery:"bucket"`
			N       int64  `bigquery:"n"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		counts, ok := out[row.RouteID]
		if !ok {
			counts = make([]int64, 96)
			out[row.RouteID] = counts
		}
		counts[row.Bucket] = row.N
	}
	return out, nil
}

// buildVolumeHistogram collapses the raw per-route 96-bucket map into a
// top-N (by daily total) series plus a single "Other" rollup, attaching
// colors from the route-color map. Limited non-Transbay routes are excluded
// before ranking and from the returned totals.
func buildVolumeHistogram(byRoute map[string][]int64, colors map[string]colorPair, scheduledByRoute map[string]int, topN int) volumeHistogram {
	type routeTotal struct {
		id    string
		total int64
	}
	totals := make([]routeTotal, 0, len(byRoute))
	system := make([]int64, 96)
	for rid, counts := range byRoute {
		if isLimitedRoute(rid, float64(scheduledByRoute[rid])) {
			continue
		}
		var sum int64
		for i, c := range counts {
			sum += c
			system[i] += c
		}
		totals = append(totals, routeTotal{id: rid, total: sum})
	}
	sort.Slice(totals, func(i, j int) bool {
		if totals[i].total != totals[j].total {
			return totals[i].total > totals[j].total
		}
		return totals[i].id < totals[j].id
	})

	out := volumeHistogram{Routes: make([]volumeRouteSeries, 0, topN+1), Totals: system}
	other := make([]int64, 96)
	var otherSum int64
	for i, rt := range totals {
		if i < topN {
			c := colors[rt.id]
			if c.color == "" {
				c.color = "FFFFFF"
			}
			if c.text == "" {
				c.text = "000000"
			}
			out.Routes = append(out.Routes, volumeRouteSeries{
				RouteID:   rt.id,
				Color:     c.color,
				TextColor: c.text,
				Counts:    byRoute[rt.id],
			})
			continue
		}
		for j, n := range byRoute[rt.id] {
			other[j] += n
		}
		otherSum += rt.total
	}
	if otherSum > 0 {
		out.Routes = append(out.Routes, volumeRouteSeries{
			RouteID:   "Other",
			Color:     "888888",
			TextColor: "FFFFFF",
			Counts:    other,
		})
	}
	return out
}

func isLimitedRoute(routeID string, scheduledRunsPerDay float64) bool {
	if scheduledRunsPerDay >= limitedRouteScheduledRunsPerDay || routeID == "" {
		return false
	}
	for _, r := range routeID {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') {
			return true
		}
	}
	return false
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
