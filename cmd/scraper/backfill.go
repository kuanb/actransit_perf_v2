package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
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
	backfillSourceBucket    = "ac-transit"
	backfillSourcePrefix    = "maptime/"
	backfillBatchSize       = 500
	backfillWindowStartHrPT = 3 // 03:00 PT on the target date
	backfillWindowEndHrPT   = 6 // 06:00 PT on the day after the target date
)

type backfillStats struct {
	ServiceDate          string `json:"service_date"`
	CSVsListed           int    `json:"csvs_listed"`
	CSVsRead             int    `json:"csvs_read"`
	RowsRead             int    `json:"rows_read"`
	RowsKept             int    `json:"rows_kept"`
	VehiclesObserved     int    `json:"vehicles_observed"`
	TripsReconstructed   int    `json:"trips_reconstructed"`
	StopArrivalsDetected int    `json:"stop_arrivals_detected"`
	ObsRowsDeleted       int64  `json:"obs_rows_deleted"`
	ProbeRowsDeleted     int64  `json:"probe_rows_deleted"`
	ObsRowsInserted      int    `json:"obs_rows_inserted"`
	ProbeRowsInserted    int    `json:"probe_rows_inserted"`
	StatsRegenerated     bool   `json:"stats_regenerated"`
}

// processBackfillDay rebuilds the BigQuery rows for a past service_date by
// reading per-minute GTFS-RT CSV snapshots from gs://ac-transit/maptime/.
// Idempotent: replaces (DELETE + INSERT) the entire partition for the
// target date, then regenerates daily stats. Refuses to run for today
// (or future) without force=true to avoid racing the live writer.
func processBackfillDay(ctx context.Context, serviceDate civil.Date, force bool) (*backfillStats, error) {
	stats := &backfillStats{ServiceDate: serviceDate.String()}

	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		return stats, fmt.Errorf("load PT location: %w", err)
	}

	if !force {
		todayPT := civil.DateOf(time.Now().In(loc))
		// /track-performance writes to today's partition continuously.
		// Backfill must stay clear of it; yesterday and earlier are safe.
		if !serviceDate.Before(todayPT) {
			return stats, fmt.Errorf(
				"refusing backfill for %s without force=true (today=%s PT — pick a past date)",
				serviceDate, todayPT,
			)
		}
	}

	gtfsBytes, err := readGTFSCurrentZip(ctx)
	if err != nil {
		return stats, fmt.Errorf("read gtfs zip: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(gtfsBytes), int64(len(gtfsBytes)))
	if err != nil {
		return stats, fmt.Errorf("open gtfs zip: %w", err)
	}
	active, err := loadActiveServices(zr, serviceDate)
	if err != nil {
		return stats, fmt.Errorf("load active services: %w", err)
	}
	if len(active) == 0 {
		return stats, fmt.Errorf(
			"current GTFS feed has no active services for %s — refresh GTFS or pick a date the feed covers",
			serviceDate,
		)
	}

	cache := ensureGTFSCache(ctx)
	if cache == nil {
		return stats, errors.New("gtfs cache is nil; /refresh-gtfs has not yet processed the feed")
	}

	startTS := time.Date(serviceDate.Year, serviceDate.Month, serviceDate.Day, backfillWindowStartHrPT, 0, 0, 0, loc).Unix()
	nextDay := serviceDate.AddDays(1)
	endTS := time.Date(nextDay.Year, nextDay.Month, nextDay.Day, backfillWindowEndHrPT, 0, 0, 0, loc).Unix()
	yyyymmdd := fmt.Sprintf("%04d%02d%02d", serviceDate.Year, serviceDate.Month, serviceDate.Day)

	snapshots, listed, read, rowsRead, rowsKept, err := readBackfillCSVs(ctx, startTS, endTS, yyyymmdd)
	if err != nil {
		return stats, fmt.Errorf("read csvs: %w", err)
	}
	stats.CSVsListed = listed
	stats.CSVsRead = read
	stats.RowsRead = rowsRead
	stats.RowsKept = rowsKept

	trips := reconstructTrips(snapshots, cache)
	stats.TripsReconstructed = len(trips)
	vehicleSet := make(map[string]struct{})
	for _, t := range trips {
		vehicleSet[t.VehicleID] = struct{}{}
		stats.StopArrivalsDetected += len(t.StopArrivals)
	}
	stats.VehiclesObserved = len(vehicleSet)

	// Audit log before destructive ops so it's easy to confirm scope after the fact.
	slog.Info("backfill pre-flight",
		"service_date", serviceDate.String(),
		"window_start_unix", startTS,
		"window_end_unix", endTS,
		"csvs_listed", stats.CSVsListed,
		"csvs_read", stats.CSVsRead,
		"rows_kept", stats.RowsKept,
		"trips", stats.TripsReconstructed,
		"vehicles", stats.VehiclesObserved,
	)

	now := time.Now().UTC()
	allObs := make([]tripObservationRow, 0)
	allProbes := make([]tripProbeRow, 0)
	for _, t := range trips {
		obs, probes := tripToRows(t, cache, now, false)
		allObs = append(allObs, obs...)
		allProbes = append(allProbes, probes...)
	}

	if err := deleteBackfillPartition(ctx, serviceDate, stats); err != nil {
		return stats, fmt.Errorf("delete partition: %w", err)
	}
	if err := insertBackfillRows(ctx, allObs, allProbes); err != nil {
		// Partition was just emptied. Surface so user can re-run.
		return stats, fmt.Errorf(
			"insert rows (partition for %s is now empty — rerun to repopulate): %w",
			serviceDate, err,
		)
	}
	stats.ObsRowsInserted = len(allObs)
	stats.ProbeRowsInserted = len(allProbes)

	// BQ streaming inserts can take a few seconds before SELECT sees them.
	// generateDailyStats is read-heavy, so let the buffer settle.
	time.Sleep(5 * time.Second)
	if _, err := generateDailyStats(ctx, serviceDate); err != nil {
		return stats, fmt.Errorf("generate stats: %w", err)
	}
	stats.StatsRegenerated = true
	return stats, nil
}

// readBackfillCSVs lists gs://ac-transit/maptime/{unix}.csv objects whose
// numeric filename falls in [startUnix, endUnix], reads each, and returns
// the parsed snapshots filtered to rows where start_date == yyyymmdd.
//
// Filenames are unix seconds, all 10 digits in our window, so lex ordering
// matches numeric ordering — a Prefix+StartOffset+EndOffset list scan is
// efficient. We still re-validate each name in case the bucket contains
// unrelated files.
func readBackfillCSVs(ctx context.Context, startUnix, endUnix int64, yyyymmdd string) ([]vehicleSnapshot, int, int, int, int, error) {
	var (
		snapshots []vehicleSnapshot
		listed    int
		read      int
		rowsRead  int
		rowsKept  int
	)

	bucket := gcsClient.Bucket(backfillSourceBucket)
	q := &storage.Query{
		Prefix:      backfillSourcePrefix,
		StartOffset: fmt.Sprintf("%s%010d.csv", backfillSourcePrefix, startUnix),
		EndOffset:   fmt.Sprintf("%s%010d.csv", backfillSourcePrefix, endUnix+1),
	}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, listed, read, rowsRead, rowsKept, fmt.Errorf("set attr selection: %w", err)
	}

	it := bucket.Objects(ctx, q)
	for {
		attrs, ierr := it.Next()
		if ierr == iterator.Done {
			break
		}
		if ierr != nil {
			return nil, listed, read, rowsRead, rowsKept, fmt.Errorf("list %s: %w", backfillSourcePrefix, ierr)
		}
		listed++

		base := strings.TrimPrefix(attrs.Name, backfillSourcePrefix)
		base = strings.TrimSuffix(base, ".csv")
		ts, perr := strconv.ParseInt(base, 10, 64)
		if perr != nil || ts < startUnix || ts > endUnix {
			continue
		}

		rc, oerr := bucket.Object(attrs.Name).NewReader(ctx)
		if oerr != nil {
			return nil, listed, read, rowsRead, rowsKept, fmt.Errorf("open %s: %w", attrs.Name, oerr)
		}
		body, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			return nil, listed, read, rowsRead, rowsKept, fmt.Errorf("read %s: %w", attrs.Name, rerr)
		}
		read++

		fileSnaps, fr, fk, perr := parseBackfillCSV(body, yyyymmdd)
		if perr != nil {
			slog.Warn("skipping malformed csv", "key", attrs.Name, "err", perr)
			continue
		}
		rowsRead += fr
		rowsKept += fk
		snapshots = append(snapshots, fileSnaps...)
	}
	return snapshots, listed, read, rowsRead, rowsKept, nil
}

// parseBackfillCSV reads the body of a single maptime/{ts}.csv file and
// returns vehicleSnapshots whose start_date matches yyyymmdd. The python
// scraper used pandas's to_csv() with default index=True, which writes an
// unnamed leading column with row numbers. We look up columns by header
// name so the leading anonymous column is harmlessly ignored.
func parseBackfillCSV(body []byte, yyyymmdd string) ([]vehicleSnapshot, int, int, error) {
	cr := csv.NewReader(bytes.NewReader(body))
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read header: %w", err)
	}
	if len(headers) > 0 {
		headers[0] = strings.TrimPrefix(headers[0], "\ufeff")
	}
	idx := headerIndex(headers)
	for _, k := range []string{"trip_id", "route_id", "lat", "lon", "timestamp", "vehicle_id", "start_date"} {
		if _, ok := idx[k]; !ok {
			return nil, 0, 0, fmt.Errorf("missing column %q in header", k)
		}
	}

	var out []vehicleSnapshot
	rowsRead, rowsKept := 0, 0
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// One bad row shouldn't abandon the file; skip and continue.
			continue
		}
		rowsRead++

		if col(row, idx, "start_date") != yyyymmdd {
			continue
		}
		tripID := col(row, idx, "trip_id")
		vehicleID := col(row, idx, "vehicle_id")
		if tripID == "" || vehicleID == "" {
			continue
		}

		tsInt, err := strconv.ParseInt(col(row, idx, "timestamp"), 10, 64)
		if err != nil || tsInt <= 0 {
			continue
		}
		lat, lerr := strconv.ParseFloat(col(row, idx, "lat"), 64)
		lon, oerr := strconv.ParseFloat(col(row, idx, "lon"), 64)
		if lerr != nil || oerr != nil {
			continue
		}
		bearing, _ := strconv.ParseFloat(col(row, idx, "bearing"), 64)
		speed, _ := strconv.ParseFloat(col(row, idx, "speed"), 64)

		out = append(out, vehicleSnapshot{
			VehicleID: vehicleID,
			RouteID:   col(row, idx, "route_id"),
			TripID:    tripID,
			StartDate: yyyymmdd,
			TS:        time.Unix(tsInt, 0).UTC(),
			Lat:       lat,
			Lon:       lon,
			Bearing:   bearing,
			SpeedMps:  speed,
		})
		rowsKept++
	}
	return out, rowsRead, rowsKept, nil
}

// reconstructTrips groups vehicleSnapshots by (vehicle_id, trip_id) into
// inFlightTrip records, dedups duplicate-TS probes (parked-bus thrash
// quirk that AC Transit's GTFS-RT also exhibits live), then projects
// probes onto route shapes and detects stop arrivals.
func reconstructTrips(snapshots []vehicleSnapshot, cache *gtfsCache) []inFlightTrip {
	type key struct{ vid, tid string }
	bucket := make(map[key][]vehicleSnapshot)
	for _, s := range snapshots {
		k := key{s.VehicleID, s.TripID}
		bucket[k] = append(bucket[k], s)
	}

	out := make([]inFlightTrip, 0, len(bucket))
	for k, list := range bucket {
		sort.Slice(list, func(i, j int) bool { return list[i].TS.Before(list[j].TS) })

		probes := make([]probe, 0, len(list))
		var lastTS time.Time
		for _, s := range list {
			if !lastTS.IsZero() && !s.TS.After(lastTS) {
				continue
			}
			probes = append(probes, probe{
				TS:               s.TS,
				Lat:              s.Lat,
				Lon:              s.Lon,
				BearingDeg:       s.Bearing,
				ReportedSpeedMps: s.SpeedMps,
			})
			lastTS = s.TS
		}
		if len(probes) == 0 {
			continue
		}
		out = append(out, inFlightTrip{
			VehicleID:   k.vid,
			TripID:      k.tid,
			RouteID:     list[0].RouteID,
			ServiceDate: list[0].StartDate,
			FirstSeenTS: probes[0].TS,
			LastSeenTS:  probes[len(probes)-1].TS,
			Probes:      probes,
		})
	}

	s := stateFile{InFlight: out}
	if cache != nil {
		var dummy trackStats
		projectInFlightProbes(&s, cache, &dummy)
		detectStopArrivals(&s, cache, &dummy)
	}
	return s.InFlight
}

// deleteBackfillPartition runs partition-scoped DELETEs for the target
// service_date on both BigQuery tables. Records affected-row counts when
// BigQuery returns them.
func deleteBackfillPartition(ctx context.Context, serviceDate civil.Date, stats *backfillStats) error {
	tables := []struct {
		name string
		dst  *int64
	}{
		{bqTableObs, &stats.ObsRowsDeleted},
		{bqTableProb, &stats.ProbeRowsDeleted},
	}
	for _, t := range tables {
		sql := fmt.Sprintf(
			"DELETE FROM `%s.%s.%s` WHERE service_date = \"%s\"",
			projectID, bqDatasetID, t.name, serviceDate,
		)
		job, err := bqClient.Query(sql).Run(ctx)
		if err != nil {
			return fmt.Errorf("submit delete %s: %w", t.name, err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			return fmt.Errorf("wait delete %s: %w", t.name, err)
		}
		if err := status.Err(); err != nil {
			return fmt.Errorf("delete %s: %w", t.name, err)
		}
		if status.Statistics != nil {
			if qd, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
				*t.dst = qd.NumDMLAffectedRows
			}
		}
	}
	return nil
}

func insertBackfillRows(ctx context.Context, obs []tripObservationRow, probes []tripProbeRow) error {
	dataset := bqClient.Dataset(bqDatasetID)
	for i := 0; i < len(obs); i += backfillBatchSize {
		end := i + backfillBatchSize
		if end > len(obs) {
			end = len(obs)
		}
		if err := dataset.Table(bqTableObs).Inserter().Put(ctx, obs[i:end]); err != nil {
			return fmt.Errorf("insert obs %d-%d: %w", i, end, err)
		}
	}
	for i := 0; i < len(probes); i += backfillBatchSize {
		end := i + backfillBatchSize
		if end > len(probes) {
			end = len(probes)
		}
		if err := dataset.Table(bqTableProb).Inserter().Put(ctx, probes[i:end]); err != nil {
			return fmt.Errorf("insert probes %d-%d: %w", i, end, err)
		}
	}
	return nil
}
