package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	agencyKPIMethodologyVersion = 1
	agencyKPIEarlySeconds       = -60
	agencyKPILateSeconds        = 300
)

type agencyKPIStats struct {
	MethodologyVersion int                  `json:"methodology_version"`
	ServiceOperated    serviceOperatedKPI   `json:"service_operated"`
	OnTimePerformance  onTimePerformanceKPI `json:"on_time_performance"`
}

type serviceOperatedKPI struct {
	ScheduledTrips  int      `json:"scheduled_trips"`
	OperatedTrips   int      `json:"operated_trips"`
	PartialTrips    int      `json:"partial_trips"`
	OperatedPct     *float64 `json:"operated_pct,omitempty"`
	PartialOfRunPct *float64 `json:"partial_of_operated_pct,omitempty"`
}

type onTimePerformanceKPI struct {
	OnTimeTimepoints    int64    `json:"on_time_timepoints"`
	OperatedTimepoints  int64    `json:"operated_timepoints"`
	ScheduledTimepoints int64    `json:"scheduled_timepoints"`
	OfOperatedPct       *float64 `json:"of_operated_pct,omitempty"`
	OfScheduledPct      *float64 `json:"of_scheduled_pct,omitempty"`
}

type kpiTimepointPlan struct {
	ByTrip map[string][]scheduledStopKey
}

type observedTripProgress struct {
	LastStopSequence int64
	HasStopSequence  bool
}

func calculateDailyAgencyKPI(
	ctx context.Context,
	zr *zip.Reader,
	serviceDate civil.Date,
	scheduledTripRoute map[string]string,
	stopPlan *scheduledStopPlan,
	arrivalProgress map[string]int64,
) (agencyKPIStats, map[string]agencyKPIStats, error) {
	timepointPlan, err := loadKPITimepointPlan(zr, scheduledTripRoute)
	if err != nil {
		return agencyKPIStats{}, nil, fmt.Errorf("load KPI timepoints: %w", err)
	}
	observedTrips, err := queryObservedProbeTrips(ctx, serviceDate)
	if err != nil {
		return agencyKPIStats{}, nil, fmt.Errorf("query observed trips: %w", err)
	}
	onTimeStops, err := queryKPIOnTimeStops(ctx, serviceDate)
	if err != nil {
		return agencyKPIStats{}, nil, fmt.Errorf("query on-time stops: %w", err)
	}
	return buildAgencyKPIStats(
		scheduledTripRoute,
		stopPlan,
		timepointPlan,
		observedTrips,
		arrivalProgress,
		onTimeStops,
	)
}

func loadKPITimepointPlan(zr *zip.Reader, tripRoutes map[string]string) (*kpiTimepointPlan, error) {
	type stop struct {
		key           scheduledStopKey
		timepoint     bool
		pickupAllowed bool
		hasSchedule   bool
	}

	cr, rc, headers, err := openZipCSV(zr, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	idx := headerIndex(headers)
	byTrip := make(map[string][]stop)
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
		byTrip[tripID] = append(byTrip[tripID], stop{
			key:           scheduledStopKey{TripID: tripID, StopSequence: sequence},
			timepoint:     col(row, idx, "timepoint") == "1",
			pickupAllowed: col(row, idx, "pickup_type") != "1",
			hasSchedule:   col(row, idx, "arrival_time") != "",
		})
	}

	plan := &kpiTimepointPlan{
		ByTrip: make(map[string][]scheduledStopKey),
	}
	for tripID, stops := range byTrip {
		sort.Slice(stops, func(i, j int) bool {
			return stops[i].key.StopSequence < stops[j].key.StopSequence
		})
		for i, stop := range stops {
			if i == 0 || i == len(stops)-1 || !stop.timepoint || !stop.pickupAllowed || !stop.hasSchedule {
				continue
			}
			plan.ByTrip[tripID] = append(plan.ByTrip[tripID], stop.key)
		}
	}
	return plan, nil
}

func queryObservedProbeTrips(ctx context.Context, serviceDate civil.Date) (map[string]observedTripProgress, error) {
	q := bqClient.Query(fmt.Sprintf(`
		SELECT trip_id, MAX(nearest_stop_seq) AS last_stop_sequence
		FROM `+"`%s.actransit.trip_probes`"+`
		WHERE service_date = "%s"
		GROUP BY trip_id
	`, projectID, serviceDate))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]observedTripProgress)
	for {
		var row struct {
			TripID           string             `bigquery:"trip_id"`
			LastStopSequence bigquery.NullInt64 `bigquery:"last_stop_sequence"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out[row.TripID] = observedTripProgress{
			LastStopSequence: row.LastStopSequence.Int64,
			HasStopSequence:  row.LastStopSequence.Valid,
		}
	}
	return out, nil
}

func queryKPIOnTimeStops(ctx context.Context, serviceDate civil.Date) (map[scheduledStopKey]struct{}, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT trip_id, stop_sequence
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND delay_seconds BETWEEN %d AND %d
	`, dedupedDayObservationsCTE(serviceDate), agencyKPIEarlySeconds, agencyKPILateSeconds))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[scheduledStopKey]struct{})
	for {
		var row scheduledStopKey
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out[row] = struct{}{}
	}
	return out, nil
}

func buildAgencyKPIStats(
	scheduledTripRoute map[string]string,
	stopPlan *scheduledStopPlan,
	timepointPlan *kpiTimepointPlan,
	observedTrips map[string]observedTripProgress,
	arrivalProgress map[string]int64,
	onTimeStops map[scheduledStopKey]struct{},
) (agencyKPIStats, map[string]agencyKPIStats, error) {
	routes := make(map[string]agencyKPIStats)
	system := emptyAgencyKPIStats()
	for tripID, routeID := range scheduledTripRoute {
		route := routes[routeID]
		if route.MethodologyVersion == 0 {
			route = emptyAgencyKPIStats()
		}
		route.ServiceOperated.ScheduledTrips++
		system.ServiceOperated.ScheduledTrips++

		observed, operated := observedTrips[tripID]
		if operated {
			route.ServiceOperated.OperatedTrips++
			system.ServiceOperated.OperatedTrips++
			lastSequence := observed.LastStopSequence
			hasProgress := observed.HasStopSequence
			if sequence, ok := arrivalProgress[tripID]; ok && (!hasProgress || sequence > lastSequence) {
				lastSequence = sequence
				hasProgress = true
			}
			if lastBoarding, ok := stopPlan.LastBoardingSequence[tripID]; ok && (!hasProgress || lastSequence < lastBoarding) {
				route.ServiceOperated.PartialTrips++
				system.ServiceOperated.PartialTrips++
			}
		}

		for _, key := range timepointPlan.ByTrip[tripID] {
			route.OnTimePerformance.ScheduledTimepoints++
			system.OnTimePerformance.ScheduledTimepoints++
			if !operated {
				continue
			}
			route.OnTimePerformance.OperatedTimepoints++
			system.OnTimePerformance.OperatedTimepoints++
			if _, ok := onTimeStops[key]; ok {
				route.OnTimePerformance.OnTimeTimepoints++
				system.OnTimePerformance.OnTimeTimepoints++
			}
		}
		routes[routeID] = route
	}
	finalizeAgencyKPIStats(&system)
	for routeID, route := range routes {
		finalizeAgencyKPIStats(&route)
		routes[routeID] = route
	}
	return system, routes, nil
}

func emptyAgencyKPIStats() agencyKPIStats {
	return agencyKPIStats{MethodologyVersion: agencyKPIMethodologyVersion}
}

func finalizeAgencyKPIStats(stats *agencyKPIStats) {
	stats.MethodologyVersion = agencyKPIMethodologyVersion
	stats.ServiceOperated.OperatedPct = ratioPct(stats.ServiceOperated.OperatedTrips, stats.ServiceOperated.ScheduledTrips)
	stats.ServiceOperated.PartialOfRunPct = ratioPct(stats.ServiceOperated.PartialTrips, stats.ServiceOperated.OperatedTrips)
	stats.OnTimePerformance.OfOperatedPct = ratioPct64(stats.OnTimePerformance.OnTimeTimepoints, stats.OnTimePerformance.OperatedTimepoints)
	stats.OnTimePerformance.OfScheduledPct = ratioPct64(stats.OnTimePerformance.OnTimeTimepoints, stats.OnTimePerformance.ScheduledTimepoints)
}

func ratioPct(numerator, denominator int) *float64 {
	if denominator == 0 {
		return nil
	}
	value := round1(100 * float64(numerator) / float64(denominator))
	return &value
}

func ratioPct64(numerator, denominator int64) *float64 {
	if denominator == 0 {
		return nil
	}
	value := round1(100 * float64(numerator) / float64(denominator))
	return &value
}

func aggregateAgencyKPIStats(values []agencyKPIStats) agencyKPIStats {
	out := emptyAgencyKPIStats()
	for _, value := range values {
		if value.MethodologyVersion != agencyKPIMethodologyVersion {
			continue
		}
		out.ServiceOperated.ScheduledTrips += value.ServiceOperated.ScheduledTrips
		out.ServiceOperated.OperatedTrips += value.ServiceOperated.OperatedTrips
		out.ServiceOperated.PartialTrips += value.ServiceOperated.PartialTrips
		out.OnTimePerformance.OnTimeTimepoints += value.OnTimePerformance.OnTimeTimepoints
		out.OnTimePerformance.OperatedTimepoints += value.OnTimePerformance.OperatedTimepoints
		out.OnTimePerformance.ScheduledTimepoints += value.OnTimePerformance.ScheduledTimepoints
	}
	finalizeAgencyKPIStats(&out)
	return out
}
