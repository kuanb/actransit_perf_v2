package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const (
	backfillSourceBucket    = "ac-transit"
	backfillSourcePrefix    = "maptime/"
	backfillWindowStartHrPT = 3 // 03:00 PT on the target date
	backfillWindowEndHrPT   = 6 // 06:00 PT on the day after the target date
	// Concurrent GCS object reads during backfill. Each maptime/{ts}.csv
	// is small (a few hundred KB) but the bucket has ~1500 of them per
	// day's window; sequential reads dominate wall time at ~5–10 min.
	// 10 workers brings that to <2 min with negligible memory pressure.
	backfillWorkerCount = 10
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

	if err := writeBackfillPartition(ctx, serviceDate, allObs, allProbes); err != nil {
		return stats, fmt.Errorf("write partition: %w", err)
	}
	stats.ObsRowsInserted = len(allObs)
	stats.ProbeRowsInserted = len(allProbes)

	if _, err := generateDailyStats(ctx, serviceDate); err != nil {
		return stats, fmt.Errorf("generate stats: %w", err)
	}
	stats.StatsRegenerated = true
	return stats, nil
}

// readBackfillCSVs lists gs://ac-transit/maptime/{unix}.csv objects whose
// numeric filename falls in [startUnix, endUnix] and reads each
// concurrently (worker pool), returning the parsed snapshots filtered
// to rows where start_date == yyyymmdd. Snapshot order isn't preserved;
// reconstructTrips sorts by TS downstream.
//
// Filenames are unix seconds, all 10 digits in our window, so lex
// ordering matches numeric ordering — a Prefix+StartOffset+EndOffset
// list scan is efficient. We still re-validate each name in case the
// bucket contains unrelated files.
func readBackfillCSVs(ctx context.Context, startUnix, endUnix int64, yyyymmdd string) ([]vehicleSnapshot, int, int, int, int, error) {
	bucket := gcsClient.Bucket(backfillSourceBucket)
	q := &storage.Query{
		Prefix:      backfillSourcePrefix,
		StartOffset: fmt.Sprintf("%s%010d.csv", backfillSourcePrefix, startUnix),
		EndOffset:   fmt.Sprintf("%s%010d.csv", backfillSourcePrefix, endUnix+1),
	}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, 0, 0, 0, 0, fmt.Errorf("set attr selection: %w", err)
	}

	var (
		mu        sync.Mutex
		snapshots []vehicleSnapshot
		listed    int
		read      int
		rowsRead  int
		rowsKept  int
	)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(backfillWorkerCount)

	fetchAndParse := func(name string) error {
		base := strings.TrimPrefix(name, backfillSourcePrefix)
		base = strings.TrimSuffix(base, ".csv")
		ts, perr := strconv.ParseInt(base, 10, 64)
		if perr != nil || ts < startUnix || ts > endUnix {
			return nil
		}
		rc, err := bucket.Object(name).NewReader(gctx)
		if err != nil {
			return fmt.Errorf("open %s: %w", name, err)
		}
		body, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		fileSnaps, fr, fk, perr := parseBackfillCSV(body, yyyymmdd)
		if perr != nil {
			// Per-file parse failures are non-fatal: log and move on.
			// A network error from GCS would have been caught above.
			slog.Warn("skipping malformed csv", "key", name, "err", perr)
			return nil
		}
		mu.Lock()
		read++
		rowsRead += fr
		rowsKept += fk
		snapshots = append(snapshots, fileSnaps...)
		mu.Unlock()
		return nil
	}

	it := bucket.Objects(gctx, q)
	for {
		attrs, ierr := it.Next()
		if ierr == iterator.Done {
			break
		}
		if ierr != nil {
			// Drain in-flight workers before returning so we don't leak goroutines.
			_ = g.Wait()
			return nil, listed, read, rowsRead, rowsKept, fmt.Errorf("list %s: %w", backfillSourcePrefix, ierr)
		}
		listed++
		name := attrs.Name
		g.Go(func() error { return fetchAndParse(name) })
	}

	if err := g.Wait(); err != nil {
		return nil, listed, read, rowsRead, rowsKept, err
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

// writeBackfillPartition atomically replaces the target service_date's
// partition on both BigQuery tables using load jobs against the partition
// decorator (table$YYYYMMDD) with WriteTruncate disposition.
//
// This replaces an earlier streaming-insert + DML-DELETE pair. Streaming
// inserts deposit rows into a "streaming buffer" — a hot in-memory layer
// queryable immediately but not modifiable via DML for ~30–90 minutes
// after the most recent stream into that partition. A re-backfill within
// that window would fail at the DELETE step. Load jobs bypass the
// streaming buffer entirely, so backfill is safely idempotent and
// re-runnable at any cadence.
func writeBackfillPartition(ctx context.Context, sd civil.Date, obs []tripObservationRow, probes []tripProbeRow) error {
	if err := loadIntoPartition(ctx, bqTableObs, sd, obs); err != nil {
		return fmt.Errorf("load %s: %w", bqTableObs, err)
	}
	if err := loadIntoPartition(ctx, bqTableProb, sd, probes); err != nil {
		return fmt.Errorf("load %s: %w", bqTableProb, err)
	}
	return nil
}

func loadIntoPartition[T any](ctx context.Context, table string, sd civil.Date, rows []T) error {
	if len(rows) == 0 {
		// Refusing to no-op silently — a 0-row backfill is suspicious and
		// would leave the partition's prior contents untouched, which is
		// the opposite of the WriteTruncate semantics callers expect.
		return fmt.Errorf("0 rows to load for %s on %s — backfill produced no data?", table, sd)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range rows {
		if err := enc.Encode(&rows[i]); err != nil {
			return fmt.Errorf("encode row %d: %w", i, err)
		}
	}

	src := bigquery.NewReaderSource(&buf)
	src.SourceFormat = bigquery.JSON

	decorated := fmt.Sprintf("%s$%04d%02d%02d", table, sd.Year, sd.Month, sd.Day)
	loader := bqClient.Dataset(bqDatasetID).Table(decorated).LoaderFrom(src)
	loader.WriteDisposition = bigquery.WriteTruncate
	loader.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("submit load: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("wait load: %w", err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("load failed: %w", err)
	}
	return nil
}
