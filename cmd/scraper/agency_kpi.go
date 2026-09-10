package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	agencyKPIMethodologyVersion = 2
	agencyKPIEarlySeconds       = -60
	agencyKPILateSeconds        = 300
)

var agencyKPITimePeriods = []struct {
	ID        string
	StartHour int
	EndHour   int
}{
	{ID: "owl", StartHour: 0, EndHour: 6},
	{ID: "morning", StartHour: 6, EndHour: 10},
	{ID: "midday", StartHour: 10, EndHour: 15},
	{ID: "afternoon", StartHour: 15, EndHour: 19},
	{ID: "evening", StartHour: 19, EndHour: 24},
}

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

type agencyKPIBreakdown map[string]map[string]agencyKPIStats

type kpiScheduledTimepoint struct {
	Key              scheduledStopKey
	ScheduledSeconds int
}

type kpiTimepointPlan struct {
	ByTrip           map[string][]kpiScheduledTimepoint
	TripStartSeconds map[string]int
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
) (agencyKPIStats, map[string]agencyKPIStats, map[string]agencyKPIBreakdown, error) {
	timepointPlan, err := loadKPITimepointPlan(zr, scheduledTripRoute)
	if err != nil {
		return agencyKPIStats{}, nil, nil, fmt.Errorf("load KPI timepoints: %w", err)
	}
	observedTrips, err := queryObservedProbeTrips(ctx, serviceDate)
	if err != nil {
		return agencyKPIStats{}, nil, nil, fmt.Errorf("query observed trips: %w", err)
	}
	onTimeStops, err := queryKPIOnTimeStops(ctx, serviceDate)
	if err != nil {
		return agencyKPIStats{}, nil, nil, fmt.Errorf("query on-time stops: %w", err)
	}
	system, routes, breakdowns, err := buildAgencyKPIStats(
		scheduledTripRoute,
		stopPlan,
		timepointPlan,
		observedTrips,
		arrivalProgress,
		onTimeStops,
		agencyKPIDayType(serviceDate),
	)
	return system, routes, breakdowns, err
}

func loadKPITimepointPlan(zr *zip.Reader, tripRoutes map[string]string) (*kpiTimepointPlan, error) {
	type stop struct {
		key              scheduledStopKey
		timepoint        bool
		pickupAllowed    bool
		startSeconds     int
		hasStart         bool
		scheduledSeconds int
		hasSchedule      bool
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
		arrivalSeconds, hasArrival, err := parseGTFSTimeSeconds(col(row, idx, "arrival_time"))
		if err != nil {
			return nil, fmt.Errorf("trip %s has invalid arrival_time: %w", tripID, err)
		}
		departureSeconds, hasDeparture, err := parseGTFSTimeSeconds(col(row, idx, "departure_time"))
		if err != nil {
			return nil, fmt.Errorf("trip %s has invalid departure_time: %w", tripID, err)
		}
		startSeconds := departureSeconds
		hasStart := hasDeparture
		if !hasStart && hasArrival {
			startSeconds = arrivalSeconds
			hasStart = true
		}
		byTrip[tripID] = append(byTrip[tripID], stop{
			key:              scheduledStopKey{TripID: tripID, StopSequence: sequence},
			timepoint:        col(row, idx, "timepoint") == "1",
			pickupAllowed:    col(row, idx, "pickup_type") != "1",
			startSeconds:     startSeconds,
			hasStart:         hasStart,
			scheduledSeconds: arrivalSeconds,
			hasSchedule:      hasArrival,
		})
	}

	plan := &kpiTimepointPlan{
		ByTrip:           make(map[string][]kpiScheduledTimepoint),
		TripStartSeconds: make(map[string]int),
	}
	for tripID, stops := range byTrip {
		sort.Slice(stops, func(i, j int) bool {
			return stops[i].key.StopSequence < stops[j].key.StopSequence
		})
		for _, stop := range stops {
			if !stop.hasStart {
				continue
			}
			plan.TripStartSeconds[tripID] = stop.startSeconds
			break
		}
		if _, ok := plan.TripStartSeconds[tripID]; !ok {
			return nil, fmt.Errorf("trip %s has no scheduled stop time", tripID)
		}
		for i, stop := range stops {
			if i == 0 || i == len(stops)-1 || !stop.timepoint || !stop.pickupAllowed || !stop.hasSchedule {
				continue
			}
			plan.ByTrip[tripID] = append(plan.ByTrip[tripID], kpiScheduledTimepoint{
				Key:              stop.key,
				ScheduledSeconds: stop.scheduledSeconds,
			})
		}
	}
	for tripID := range tripRoutes {
		if _, ok := plan.TripStartSeconds[tripID]; !ok {
			return nil, fmt.Errorf("trip %s has no stop_times row", tripID)
		}
	}
	return plan, nil
}

func parseGTFSTimeSeconds(value string) (int, bool, error) {
	if value == "" {
		return 0, false, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 3 {
		return 0, false, fmt.Errorf("expected HH:MM:SS, got %q", value)
	}
	values := [3]int{}
	for i, part := range parts {
		parsed, err := strconv.Atoi(part)
		if err != nil {
			return 0, false, err
		}
		values[i] = parsed
	}
	hour, minute, second := values[0], values[1], values[2]
	if hour < 0 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return 0, false, fmt.Errorf("out of range %q", value)
	}
	return hour*60*60 + minute*60 + second, true, nil
}

func agencyKPITimePeriod(seconds int) string {
	hour := (seconds % (24 * 60 * 60)) / (60 * 60)
	for _, period := range agencyKPITimePeriods {
		if hour >= period.StartHour && hour < period.EndHour {
			return period.ID
		}
	}
	return ""
}

func agencyKPIDayType(serviceDate civil.Date) string {
	weekday := civilWeekday(serviceDate)
	if weekday == 0 || weekday == 6 {
		return "weekend"
	}
	return "weekday"
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
	dayType string,
) (agencyKPIStats, map[string]agencyKPIStats, map[string]agencyKPIBreakdown, error) {
	routes := make(map[string]agencyKPIStats)
	routePeriods := make(map[string]map[string]agencyKPIStats)
	system := emptyAgencyKPIStats()
	for tripID, routeID := range scheduledTripRoute {
		route := routes[routeID]
		if route.MethodologyVersion == 0 {
			route = emptyAgencyKPIStats()
		}
		route.ServiceOperated.ScheduledTrips++
		system.ServiceOperated.ScheduledTrips++
		periodID := agencyKPITimePeriod(timepointPlan.TripStartSeconds[tripID])
		periods := routePeriods[routeID]
		if periods == nil {
			periods = make(map[string]agencyKPIStats)
			routePeriods[routeID] = periods
		}
		period := periods[periodID]
		if period.MethodologyVersion == 0 {
			period = emptyAgencyKPIStats()
		}
		period.ServiceOperated.ScheduledTrips++

		observed, operated := observedTrips[tripID]
		partial := false
		if operated {
			route.ServiceOperated.OperatedTrips++
			system.ServiceOperated.OperatedTrips++
			period.ServiceOperated.OperatedTrips++
			lastSequence := observed.LastStopSequence
			hasProgress := observed.HasStopSequence
			if sequence, ok := arrivalProgress[tripID]; ok && (!hasProgress || sequence > lastSequence) {
				lastSequence = sequence
				hasProgress = true
			}
			if lastBoarding, ok := stopPlan.LastBoardingSequence[tripID]; ok && (!hasProgress || lastSequence < lastBoarding) {
				route.ServiceOperated.PartialTrips++
				system.ServiceOperated.PartialTrips++
				partial = true
			}
		}
		if partial {
			period.ServiceOperated.PartialTrips++
		}
		periods[periodID] = period

		for _, timepoint := range timepointPlan.ByTrip[tripID] {
			route.OnTimePerformance.ScheduledTimepoints++
			system.OnTimePerformance.ScheduledTimepoints++
			timepointPeriodID := agencyKPITimePeriod(timepoint.ScheduledSeconds)
			timepointPeriod := periods[timepointPeriodID]
			if timepointPeriod.MethodologyVersion == 0 {
				timepointPeriod = emptyAgencyKPIStats()
			}
			timepointPeriod.OnTimePerformance.ScheduledTimepoints++
			if !operated {
				periods[timepointPeriodID] = timepointPeriod
				continue
			}
			route.OnTimePerformance.OperatedTimepoints++
			system.OnTimePerformance.OperatedTimepoints++
			timepointPeriod.OnTimePerformance.OperatedTimepoints++
			if _, ok := onTimeStops[timepoint.Key]; ok {
				route.OnTimePerformance.OnTimeTimepoints++
				system.OnTimePerformance.OnTimeTimepoints++
				timepointPeriod.OnTimePerformance.OnTimeTimepoints++
			}
			periods[timepointPeriodID] = timepointPeriod
		}
		routes[routeID] = route
	}
	breakdowns := make(map[string]agencyKPIBreakdown, len(routePeriods))
	finalizeAgencyKPIStats(&system)
	for routeID, route := range routes {
		finalizeAgencyKPIStats(&route)
		routes[routeID] = route
		periods := routePeriods[routeID]
		for periodID, period := range periods {
			finalizeAgencyKPIStats(&period)
			periods[periodID] = period
		}
		breakdowns[routeID] = agencyKPIBreakdown{dayType: periods}
	}
	return system, routes, breakdowns, nil
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
		addAgencyKPICounts(&out, value)
	}
	finalizeAgencyKPIStats(&out)
	return out
}

func aggregateAgencyKPIBreakdowns(values []agencyKPIBreakdown) agencyKPIBreakdown {
	out := make(agencyKPIBreakdown)
	for _, value := range values {
		for dayType, periods := range value {
			if out[dayType] == nil {
				out[dayType] = make(map[string]agencyKPIStats)
			}
			for periodID, period := range periods {
				if period.MethodologyVersion != agencyKPIMethodologyVersion {
					continue
				}
				combined := out[dayType][periodID]
				if combined.MethodologyVersion == 0 {
					combined = emptyAgencyKPIStats()
				}
				addAgencyKPICounts(&combined, period)
				out[dayType][periodID] = combined
			}
		}
	}
	for dayType, periods := range out {
		for periodID, period := range periods {
			finalizeAgencyKPIStats(&period)
			periods[periodID] = period
		}
		out[dayType] = periods
	}
	return out
}

func addAgencyKPICounts(dst *agencyKPIStats, value agencyKPIStats) {
	dst.ServiceOperated.ScheduledTrips += value.ServiceOperated.ScheduledTrips
	dst.ServiceOperated.OperatedTrips += value.ServiceOperated.OperatedTrips
	dst.ServiceOperated.PartialTrips += value.ServiceOperated.PartialTrips
	dst.OnTimePerformance.OnTimeTimepoints += value.OnTimePerformance.OnTimeTimepoints
	dst.OnTimePerformance.OperatedTimepoints += value.OnTimePerformance.OperatedTimepoints
	dst.OnTimePerformance.ScheduledTimepoints += value.OnTimePerformance.ScheduledTimepoints
}
