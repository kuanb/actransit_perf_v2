package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

const (
	bqDatasetID = "actransit"
	bqTableObs  = "trip_observations"
	bqTableProb = "trip_probes"
)

var bqClient *bigquery.Client

type tripObservationRow struct {
	ServiceDate      civil.Date              `bigquery:"service_date"`
	RouteID          string                  `bigquery:"route_id"`
	TripID           string                  `bigquery:"trip_id"`
	VehicleID        string                  `bigquery:"vehicle_id"`
	StopSequence     int64                   `bigquery:"stop_sequence"`
	StopID           string                  `bigquery:"stop_id"`
	ScheduledArrival bigquery.NullTimestamp  `bigquery:"scheduled_arrival"`
	ActualArrival    bigquery.NullTimestamp  `bigquery:"actual_arrival"`
	DelaySeconds     bigquery.NullInt64      `bigquery:"delay_seconds"`
	LegDistanceM     bigquery.NullFloat64    `bigquery:"leg_distance_m"`
	LegDurationS     bigquery.NullFloat64    `bigquery:"leg_duration_s"`
	LegAvgSpeedMps   bigquery.NullFloat64    `bigquery:"leg_avg_speed_mps"`
	IsStale          bool                    `bigquery:"is_stale"`
	IngestedAt       time.Time               `bigquery:"ingested_at"`
}

type tripProbeRow struct {
	ServiceDate      civil.Date           `bigquery:"service_date"`
	RouteID          string               `bigquery:"route_id"`
	TripID           string               `bigquery:"trip_id"`
	VehicleID        string               `bigquery:"vehicle_id"`
	ObservedAt       time.Time            `bigquery:"observed_at"`
	Lat              float64              `bigquery:"lat"`
	Lon              float64              `bigquery:"lon"`
	BearingDeg       bigquery.NullFloat64 `bigquery:"bearing_deg"`
	ReportedSpeedMps bigquery.NullFloat64 `bigquery:"reported_speed_mps"`
	DistAlongRouteM  float64              `bigquery:"dist_along_route_m"`
	NearestStopSeq   bigquery.NullInt64   `bigquery:"nearest_stop_seq"`
	IngestedAt       time.Time            `bigquery:"ingested_at"`
}

// tripToRows converts a finalized in-flight trip into BigQuery rows.
// Pure: callable without a BigQuery client. Returns observation rows
// (one per stop on the trip's GTFS-static stop_times) and probe rows
// (one per probe currently in state). If the trip's route or trip_id is
// missing from the cache, observation rows are omitted; probe rows are
// always returned.
func tripToRows(t inFlightTrip, cache *gtfsCache, ingestedAt time.Time, isStale bool) ([]tripObservationRow, []tripProbeRow) {
	serviceDate := parseServiceDate(t.ServiceDate)
	probes := buildProbeRows(t, serviceDate, ingestedAt)
	obs := buildObservationRows(t, cache, serviceDate, ingestedAt, isStale)
	return obs, probes
}

func buildProbeRows(t inFlightTrip, serviceDate civil.Date, ingestedAt time.Time) []tripProbeRow {
	out := make([]tripProbeRow, 0, len(t.Probes))
	for _, p := range t.Probes {
		row := tripProbeRow{
			ServiceDate:     serviceDate,
			RouteID:         t.RouteID,
			TripID:          t.TripID,
			VehicleID:       t.VehicleID,
			ObservedAt:      p.TS,
			Lat:             p.Lat,
			Lon:             p.Lon,
			DistAlongRouteM: p.DistAlongRouteM,
			IngestedAt:      ingestedAt,
		}
		if p.BearingDeg != 0 {
			row.BearingDeg = bigquery.NullFloat64{Float64: p.BearingDeg, Valid: true}
		}
		if p.ReportedSpeedMps != 0 {
			row.ReportedSpeedMps = bigquery.NullFloat64{Float64: p.ReportedSpeedMps, Valid: true}
		}
		if p.NearestStopSeq != 0 {
			row.NearestStopSeq = bigquery.NullInt64{Int64: int64(p.NearestStopSeq), Valid: true}
		}
		out = append(out, row)
	}
	return out
}

func buildObservationRows(t inFlightTrip, cache *gtfsCache, serviceDate civil.Date, ingestedAt time.Time, isStale bool) []tripObservationRow {
	if cache == nil {
		return nil
	}
	route, ok := cache.Routes[t.RouteID]
	if !ok {
		return nil
	}
	trip, ok := route.Trips[t.TripID]
	if !ok {
		return nil
	}

	out := make([]tripObservationRow, 0, len(trip.StopTimes))
	prevActual := bigquery.NullTimestamp{}
	prevDist := 0.0

	for _, st := range trip.StopTimes {
		row := tripObservationRow{
			ServiceDate:  serviceDate,
			RouteID:      t.RouteID,
			TripID:       t.TripID,
			VehicleID:    t.VehicleID,
			StopSequence: int64(st.StopSequence),
			StopID:       st.StopID,
			IsStale:      isStale,
			IngestedAt:   ingestedAt,
		}

		if sched := parseScheduledArrival(serviceDate, st.ArrivalTime); !sched.IsZero() {
			row.ScheduledArrival = bigquery.NullTimestamp{Timestamp: sched, Valid: true}
		}

		if actual, ok := t.StopArrivals[st.StopSequence]; ok {
			row.ActualArrival = bigquery.NullTimestamp{Timestamp: actual, Valid: true}
			if row.ScheduledArrival.Valid {
				delay := int64(actual.Sub(row.ScheduledArrival.Timestamp).Seconds())
				row.DelaySeconds = bigquery.NullInt64{Int64: delay, Valid: true}
			}
			if prevActual.Valid {
				durSec := actual.Sub(prevActual.Timestamp).Seconds()
				distM := st.DistAlongRoute - prevDist
				if durSec > 0 && distM > 0 {
					row.LegDurationS = bigquery.NullFloat64{Float64: durSec, Valid: true}
					row.LegDistanceM = bigquery.NullFloat64{Float64: distM, Valid: true}
					row.LegAvgSpeedMps = bigquery.NullFloat64{Float64: distM / durSec, Valid: true}
				}
			}
			prevActual = row.ActualArrival
			prevDist = st.DistAlongRoute
		}

		out = append(out, row)
	}
	return out
}

// parseServiceDate converts GTFS-RT's "YYYYMMDD" into a civil.Date for BQ.
// Returns the zero value if input is malformed.
func parseServiceDate(s string) civil.Date {
	if len(s) != 8 {
		return civil.Date{}
	}
	t, err := time.Parse("20060102", s)
	if err != nil {
		return civil.Date{}
	}
	return civil.DateOf(t)
}

// parseScheduledArrival combines a GTFS-static "HH:MM:SS" arrival time
// (which can exceed 24:00:00 for trips that cross midnight) with the
// service_date to produce a TIMESTAMP in America/Los_Angeles. Returns
// the zero time if either input is unusable.
func parseScheduledArrival(serviceDate civil.Date, gtfsTime string) time.Time {
	if gtfsTime == "" || serviceDate.IsZero() {
		return time.Time{}
	}
	var hh, mm, ss int
	if _, err := fmt.Sscanf(gtfsTime, "%d:%d:%d", &hh, &mm, &ss); err != nil {
		return time.Time{}
	}
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	day := time.Date(serviceDate.Year, time.Month(serviceDate.Month), serviceDate.Day, 0, 0, 0, 0, loc)
	return day.Add(time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second).UTC()
}

// writeFinalizedTrips streams observation and probe rows to BigQuery.
// Failures are logged but don't propagate — finalization is best-effort
// so a transient BQ outage doesn't block state evolution. Returns counts
// of rows attempted (not necessarily inserted on partial failure).
func writeFinalizedTrips(ctx context.Context, trips []inFlightTrip, cache *gtfsCache, now time.Time) (obsCount, probeCount int, err error) {
	if bqClient == nil || len(trips) == 0 {
		return 0, 0, nil
	}
	var allObs []tripObservationRow
	var allProbes []tripProbeRow
	for _, t := range trips {
		obs, probes := tripToRows(t, cache, now, true)
		allObs = append(allObs, obs...)
		allProbes = append(allProbes, probes...)
	}

	dataset := bqClient.Dataset(bqDatasetID)

	if len(allObs) > 0 {
		if err := dataset.Table(bqTableObs).Inserter().Put(ctx, allObs); err != nil {
			return len(allObs), len(allProbes), fmt.Errorf("insert observations: %w", err)
		}
	}
	if len(allProbes) > 0 {
		if err := dataset.Table(bqTableProb).Inserter().Put(ctx, allProbes); err != nil {
			return len(allObs), len(allProbes), fmt.Errorf("insert probes: %w", err)
		}
	}
	return len(allObs), len(allProbes), nil
}
