package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	schoolStatsMethodologyVersion = 2
	schoolStatsPrefix             = "stats/schools/"
	schoolStatsLatestKey          = schoolStatsPrefix + "latest.json"
	schoolStatsIndexKey           = schoolStatsPrefix + "_index.json"
	schoolArrivalGeofenceMeters   = 250.0
	schoolDepartureLateThreshold  = 15 * time.Minute
	schoolArrivalWindowBefore     = 2 * time.Hour
	schoolArrivalWindowAfter      = 30 * time.Minute
	arrivalConfidenceConfirmed    = "confirmed_250m_crossing"
	arrivalConfidenceFirstSeen    = "first_seen_inside_250m"
)

type schoolStartPeriod struct {
	ValidFrom  civil.Date
	ValidUntil civil.Date
	StartTime  string
	SourceURL  string
}

type schoolCalendarPeriod struct {
	Name          string
	StartDate     civil.Date
	EndDate       civil.Date
	NoSchoolDates []civil.Date
	SourceURL     string
}

type schoolRouteTarget struct {
	RouteID string
	StopIDs []string
}

type schoolDefinition struct {
	ID              string
	Name            string
	StartPeriods    []schoolStartPeriod
	CalendarPeriods []schoolCalendarPeriod
	Routes          []schoolRouteTarget
}

var wccusd2026SchoolYear = schoolCalendarPeriod{
	Name:      "2026-2027 school year",
	StartDate: civil.Date{Year: 2026, Month: 8, Day: 18},
	EndDate:   civil.Date{Year: 2027, Month: 6, Day: 10},
	NoSchoolDates: []civil.Date{
		{Year: 2026, Month: 9, Day: 7},
		{Year: 2026, Month: 10, Day: 12},
		{Year: 2026, Month: 11, Day: 11},
		{Year: 2026, Month: 11, Day: 23},
		{Year: 2026, Month: 11, Day: 24},
		{Year: 2026, Month: 11, Day: 25},
		{Year: 2026, Month: 11, Day: 26},
		{Year: 2026, Month: 11, Day: 27},
		{Year: 2026, Month: 12, Day: 21},
		{Year: 2026, Month: 12, Day: 22},
		{Year: 2026, Month: 12, Day: 23},
		{Year: 2026, Month: 12, Day: 24},
		{Year: 2026, Month: 12, Day: 25},
		{Year: 2026, Month: 12, Day: 28},
		{Year: 2026, Month: 12, Day: 29},
		{Year: 2026, Month: 12, Day: 30},
		{Year: 2026, Month: 12, Day: 31},
		{Year: 2027, Month: 1, Day: 1},
		{Year: 2027, Month: 1, Day: 18},
		{Year: 2027, Month: 1, Day: 19},
		{Year: 2027, Month: 2, Day: 15},
		{Year: 2027, Month: 2, Day: 16},
		{Year: 2027, Month: 2, Day: 17},
		{Year: 2027, Month: 2, Day: 18},
		{Year: 2027, Month: 2, Day: 19},
		{Year: 2027, Month: 3, Day: 26},
		{Year: 2027, Month: 4, Day: 5},
		{Year: 2027, Month: 4, Day: 6},
		{Year: 2027, Month: 4, Day: 7},
		{Year: 2027, Month: 4, Day: 8},
		{Year: 2027, Month: 4, Day: 9},
		{Year: 2027, Month: 5, Day: 28},
		{Year: 2027, Month: 5, Day: 31},
	},
	SourceURL: "https://resources.finalsite.net/images/v1781722609/wccusdnet/outbgmsodaqhv0mn49lc/SchoolCalendar2026-27Revised_1.pdf",
}

var schoolDefinitions = []schoolDefinition{
	{
		ID:   "el-cerrito-high",
		Name: "El Cerrito High School",
		StartPeriods: []schoolStartPeriod{
			{
				ValidFrom: civil.Date{Year: 2026, Month: 8, Day: 18},
				StartTime: "08:30",
				SourceURL: "https://ecgauchos.wccusd.net/our-school/bell-schedule",
			},
		},
		CalendarPeriods: []schoolCalendarPeriod{wccusd2026SchoolYear},
		Routes: []schoolRouteTarget{
			{RouteID: "667", StopIDs: []string{"826"}},
			{RouteID: "684", StopIDs: []string{"827"}},
		},
	},
	{
		ID:   "korematsu-middle",
		Name: "Korematsu Middle School",
		StartPeriods: []schoolStartPeriod{
			{
				ValidFrom: civil.Date{Year: 2026, Month: 8, Day: 18},
				StartTime: "08:30",
				SourceURL: "https://korematsu.wccusd.net/our-school/bell-schedule",
			},
		},
		CalendarPeriods: []schoolCalendarPeriod{wccusd2026SchoolYear},
		Routes: []schoolRouteTarget{
			{RouteID: "675", StopIDs: []string{"5362"}},
		},
	},
}

type schoolStats struct {
	MethodologyVersion            int              `json:"methodology_version"`
	ServiceDate                   string           `json:"service_date"`
	GeneratedAt                   time.Time        `json:"generated_at"`
	GeofenceRadiusM               float64          `json:"geofence_radius_m"`
	DepartureLateThresholdMinutes int              `json:"departure_late_threshold_minutes"`
	Schools                       []schoolDayStats `json:"schools"`
}

type schoolDayStats struct {
	SchoolID             string            `json:"school_id"`
	Name                 string            `json:"name"`
	StartTimeLocal       string            `json:"start_time_local"`
	StartTimeValidFrom   string            `json:"start_time_valid_from"`
	StartTimeValidUntil  *string           `json:"start_time_valid_until"`
	StartTimeSourceURL   string            `json:"start_time_source_url,omitempty"`
	SchoolCalendarName   string            `json:"school_calendar_name"`
	SchoolCalendarStart  string            `json:"school_calendar_start_date"`
	SchoolCalendarEnd    string            `json:"school_calendar_end_date"`
	SchoolCalendarClosed []string          `json:"school_calendar_no_school_dates"`
	SchoolCalendarSource string            `json:"school_calendar_source_url"`
	ScheduledTrips       int               `json:"scheduled_trips"`
	ObservedArrivals     int               `json:"observed_arrivals"`
	ArrivalsAfterStart   int               `json:"arrivals_after_start"`
	TripsNotObserved     int               `json:"trips_not_observed"`
	AllScheduledArrived  bool              `json:"all_scheduled_arrived"`
	Trips                []schoolTripStats `json:"trips"`
}

type schoolTripStats struct {
	RouteID                 string     `json:"route_id"`
	TripID                  string     `json:"trip_id"`
	VehicleID               string     `json:"vehicle_id,omitempty"`
	StopID                  string     `json:"stop_id"`
	ScheduledDeparture      time.Time  `json:"scheduled_departure"`
	ActualDeparture         *time.Time `json:"actual_departure,omitempty"`
	DepartureConfidence     string     `json:"departure_confidence,omitempty"`
	DepartureDelayMinutes   *float64   `json:"departure_delay_minutes,omitempty"`
	DepartureStatus         string     `json:"departure_status"`
	ScheduledArrival        time.Time  `json:"scheduled_arrival"`
	ActualArrival           *time.Time `json:"actual_arrival,omitempty"`
	ArrivalConfidence       string     `json:"arrival_confidence,omitempty"`
	ArrivalMinutesFromStart *float64   `json:"arrival_minutes_from_start,omitempty"`
	ArrivalStatus           string     `json:"arrival_status"`
	ObservedRunning         bool       `json:"observed_running"`
}

type scheduledSchoolTrip struct {
	SchoolID           string
	StartTime          time.Time
	RouteID            string
	TripID             string
	StopID             string
	Stop               gtfsStop
	ScheduledDeparture time.Time
	ScheduledArrival   time.Time
}

type schoolObservationSummary struct {
	VehicleID             string
	ActualDeparture       *time.Time
	DepartureConfidence   string
	DepartureDelayMinutes *float64
	ObservedRunning       bool
}

func generateSchoolStats(ctx context.Context, zr *zip.Reader, serviceDate civil.Date, activeServices map[string]struct{}) (*schoolStats, error) {
	if err := validateSchoolDefinitions(schoolDefinitions); err != nil {
		return nil, err
	}
	if !anySchoolInSession(serviceDate, schoolDefinitions) {
		out := buildSchoolStats(serviceDate, nil, nil, nil, schoolDefinitions)
		if err := writeSchoolStats(ctx, serviceDate, out); err != nil {
			return nil, err
		}
		return out, nil
	}
	scheduled, err := loadScheduledSchoolTrips(zr, serviceDate, activeServices, schoolDefinitions)
	if err != nil {
		return nil, fmt.Errorf("load school schedule: %w", err)
	}
	tripIDs := make([]string, 0, len(scheduled))
	seenTrips := make(map[string]struct{}, len(scheduled))
	for _, trip := range scheduled {
		if _, ok := seenTrips[trip.TripID]; ok {
			continue
		}
		seenTrips[trip.TripID] = struct{}{}
		tripIDs = append(tripIDs, trip.TripID)
	}

	observations, err := querySchoolObservations(ctx, serviceDate, tripIDs)
	if err != nil {
		return nil, fmt.Errorf("query school observations: %w", err)
	}
	probes, err := querySchoolProbes(ctx, serviceDate, tripIDs)
	if err != nil {
		return nil, fmt.Errorf("query school probes: %w", err)
	}

	out := buildSchoolStats(serviceDate, scheduled, observations, probes, schoolDefinitions)
	if err := writeSchoolStats(ctx, serviceDate, out); err != nil {
		return nil, err
	}
	return out, nil
}

func generateSchoolStatsForDate(ctx context.Context, serviceDate civil.Date) (*schoolStats, error) {
	if !anySchoolInSession(serviceDate, schoolDefinitions) {
		return generateSchoolStats(ctx, nil, serviceDate, nil)
	}
	body, err := readGTFSZipForServiceDate(ctx, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("read gtfs zip: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("open gtfs zip: %w", err)
	}
	activeServices, err := loadActiveServices(zr, serviceDate)
	if err != nil {
		return nil, fmt.Errorf("active services: %w", err)
	}
	return generateSchoolStats(ctx, zr, serviceDate, activeServices)
}

func validateSchoolDefinitions(definitions []schoolDefinition) error {
	for _, school := range definitions {
		if school.ID == "" || school.Name == "" || len(school.Routes) == 0 || len(school.StartPeriods) == 0 || len(school.CalendarPeriods) == 0 {
			return fmt.Errorf("invalid school definition %q", school.ID)
		}
		for i, period := range school.StartPeriods {
			if period.ValidFrom.IsZero() {
				return fmt.Errorf("school %s has a period without valid_from", school.ID)
			}
			if _, err := parseSchoolClock(period.StartTime); err != nil {
				return fmt.Errorf("school %s: %w", school.ID, err)
			}
			if !period.ValidUntil.IsZero() && !period.ValidFrom.Before(period.ValidUntil) {
				return fmt.Errorf("school %s has an empty start-time period", school.ID)
			}
			if i == 0 {
				continue
			}
			prev := school.StartPeriods[i-1]
			if prev.ValidUntil.IsZero() || period.ValidFrom.Before(prev.ValidUntil) {
				return fmt.Errorf("school %s has overlapping start-time periods", school.ID)
			}
		}
		for i, period := range school.CalendarPeriods {
			if period.Name == "" || period.StartDate.IsZero() || period.EndDate.IsZero() || period.SourceURL == "" {
				return fmt.Errorf("school %s has an invalid calendar period", school.ID)
			}
			if period.EndDate.Before(period.StartDate) {
				return fmt.Errorf("school %s has an empty calendar period", school.ID)
			}
			if i > 0 && !school.CalendarPeriods[i-1].EndDate.Before(period.StartDate) {
				return fmt.Errorf("school %s has overlapping calendar periods", school.ID)
			}
			for _, noSchoolDate := range period.NoSchoolDates {
				if noSchoolDate.Before(period.StartDate) || period.EndDate.Before(noSchoolDate) {
					return fmt.Errorf("school %s has a no-school date outside its calendar period", school.ID)
				}
			}
		}
	}
	return nil
}

func schoolCalendarPeriodFor(date civil.Date, periods []schoolCalendarPeriod) (schoolCalendarPeriod, bool) {
	weekday := time.Date(date.Year, time.Month(date.Month), date.Day, 0, 0, 0, 0, time.UTC).Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return schoolCalendarPeriod{}, false
	}
	for _, period := range periods {
		if date.Before(period.StartDate) || period.EndDate.Before(date) {
			continue
		}
		for _, noSchoolDate := range period.NoSchoolDates {
			if date == noSchoolDate {
				return schoolCalendarPeriod{}, false
			}
		}
		return period, true
	}
	return schoolCalendarPeriod{}, false
}

func anySchoolInSession(date civil.Date, definitions []schoolDefinition) bool {
	for _, school := range definitions {
		if _, ok := schoolStartPeriodFor(date, school.StartPeriods); !ok {
			continue
		}
		if _, ok := schoolCalendarPeriodFor(date, school.CalendarPeriods); ok {
			return true
		}
	}
	return false
}

func schoolStartPeriodFor(date civil.Date, periods []schoolStartPeriod) (schoolStartPeriod, bool) {
	for _, period := range periods {
		if date.Before(period.ValidFrom) {
			continue
		}
		if !period.ValidUntil.IsZero() && !date.Before(period.ValidUntil) {
			continue
		}
		return period, true
	}
	return schoolStartPeriod{}, false
}

func parseSchoolClock(value string) (time.Duration, error) {
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, fmt.Errorf("invalid school start time %q", value)
	}
	return time.Duration(parsed.Hour())*time.Hour + time.Duration(parsed.Minute())*time.Minute, nil
}

func schoolStartTimestamp(date civil.Date, value string) time.Time {
	return parseScheduledArrival(date, value+":00")
}

func loadScheduledSchoolTrips(zr *zip.Reader, serviceDate civil.Date, activeServices map[string]struct{}, definitions []schoolDefinition) ([]scheduledSchoolTrip, error) {
	type tripMeta struct {
		RouteID string
	}
	type stopTime struct {
		Sequence      int
		StopID        string
		ArrivalTime   string
		DepartureTime string
	}

	routeSet := make(map[string]struct{})
	for _, school := range definitions {
		if _, ok := schoolStartPeriodFor(serviceDate, school.StartPeriods); !ok {
			continue
		}
		if _, ok := schoolCalendarPeriodFor(serviceDate, school.CalendarPeriods); !ok {
			continue
		}
		for _, route := range school.Routes {
			routeSet[route.RouteID] = struct{}{}
		}
	}

	tripMetaByID := make(map[string]tripMeta)
	cr, rc, headers, err := openZipCSV(zr, "trips.txt")
	if err != nil {
		return nil, err
	}
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
		if _, ok := activeServices[col(row, idx, "service_id")]; !ok {
			continue
		}
		routeID := col(row, idx, "route_id")
		if _, ok := routeSet[routeID]; !ok {
			continue
		}
		tripMetaByID[col(row, idx, "trip_id")] = tripMeta{RouteID: routeID}
	}
	rc.Close()

	stopTimesByTrip := make(map[string][]stopTime)
	cr, rc, headers, err = openZipCSV(zr, "stop_times.txt")
	if err != nil {
		return nil, err
	}
	idx = headerIndex(headers)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			rc.Close()
			return nil, err
		}
		tripID := col(row, idx, "trip_id")
		if _, ok := tripMetaByID[tripID]; !ok {
			continue
		}
		sequence, err := strconv.Atoi(col(row, idx, "stop_sequence"))
		if err != nil {
			rc.Close()
			return nil, fmt.Errorf("trip %s invalid stop_sequence: %w", tripID, err)
		}
		stopTimesByTrip[tripID] = append(stopTimesByTrip[tripID], stopTime{
			Sequence:      sequence,
			StopID:        col(row, idx, "stop_id"),
			ArrivalTime:   col(row, idx, "arrival_time"),
			DepartureTime: col(row, idx, "departure_time"),
		})
	}
	rc.Close()

	stops := make(map[string]gtfsStop)
	cr, rc, headers, err = openZipCSV(zr, "stops.txt")
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
		lat, latErr := strconv.ParseFloat(col(row, idx, "stop_lat"), 64)
		lon, lonErr := strconv.ParseFloat(col(row, idx, "stop_lon"), 64)
		if latErr != nil || lonErr != nil {
			continue
		}
		stopID := col(row, idx, "stop_id")
		stops[stopID] = gtfsStop{StopID: stopID, StopName: col(row, idx, "stop_name"), Lat: lat, Lon: lon}
	}

	var out []scheduledSchoolTrip
	for tripID, meta := range tripMetaByID {
		tripStops := stopTimesByTrip[tripID]
		if len(tripStops) == 0 {
			continue
		}
		sort.Slice(tripStops, func(i, j int) bool { return tripStops[i].Sequence < tripStops[j].Sequence })
		originDeparture := parseScheduledArrival(serviceDate, tripStops[0].DepartureTime)
		if originDeparture.IsZero() {
			originDeparture = parseScheduledArrival(serviceDate, tripStops[0].ArrivalTime)
		}

		for _, school := range definitions {
			period, ok := schoolStartPeriodFor(serviceDate, school.StartPeriods)
			if !ok {
				continue
			}
			if _, ok := schoolCalendarPeriodFor(serviceDate, school.CalendarPeriods); !ok {
				continue
			}
			start := schoolStartTimestamp(serviceDate, period.StartTime)
			for _, route := range school.Routes {
				if route.RouteID != meta.RouteID {
					continue
				}
				stopSet := make(map[string]struct{}, len(route.StopIDs))
				for _, stopID := range route.StopIDs {
					stopSet[stopID] = struct{}{}
				}
				for _, stop := range tripStops {
					if _, ok := stopSet[stop.StopID]; !ok {
						continue
					}
					arrival := parseScheduledArrival(serviceDate, stop.ArrivalTime)
					if arrival.IsZero() || arrival.Before(start.Add(-schoolArrivalWindowBefore)) || arrival.After(start.Add(schoolArrivalWindowAfter)) {
						continue
					}
					stopDetail, ok := stops[stop.StopID]
					if !ok {
						return nil, fmt.Errorf("school stop %s missing from stops.txt", stop.StopID)
					}
					out = append(out, scheduledSchoolTrip{
						SchoolID:           school.ID,
						StartTime:          start,
						RouteID:            meta.RouteID,
						TripID:             tripID,
						StopID:             stop.StopID,
						Stop:               stopDetail,
						ScheduledDeparture: originDeparture,
						ScheduledArrival:   arrival,
					})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SchoolID != out[j].SchoolID {
			return out[i].SchoolID < out[j].SchoolID
		}
		if !out[i].ScheduledArrival.Equal(out[j].ScheduledArrival) {
			return out[i].ScheduledArrival.Before(out[j].ScheduledArrival)
		}
		if out[i].RouteID != out[j].RouteID {
			return out[i].RouteID < out[j].RouteID
		}
		return out[i].TripID < out[j].TripID
	})
	return out, nil
}

func querySchoolObservations(ctx context.Context, serviceDate civil.Date, tripIDs []string) (map[string]schoolObservationSummary, error) {
	out := make(map[string]schoolObservationSummary)
	if len(tripIDs) == 0 {
		return out, nil
	}
	q := bqClient.Query(fmt.Sprintf(`
		WITH obs AS (
		  SELECT * FROM `+"`%s.actransit.trip_observations`"+`
		  WHERE service_date = @service_date
		    AND trip_id IN UNNEST(@trip_ids)
		  QUALIFY ROW_NUMBER() OVER (
		    PARTITION BY trip_id, stop_sequence
		    ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
		  ) = 1
		)
		SELECT
		  trip_id,
		  ARRAY_AGG(vehicle_id ORDER BY stop_sequence LIMIT 1)[OFFSET(0)] AS vehicle_id,
		  MAX(actual_departure) AS actual_departure,
		  MAX(departure_delay_seconds) AS departure_delay_seconds,
		  ARRAY_AGG(departure_confidence IGNORE NULLS ORDER BY stop_sequence LIMIT 1)[SAFE_OFFSET(0)] AS departure_confidence,
		  COUNTIF(actual_arrival IS NOT NULL) > 0 AS observed_running
		FROM obs
		GROUP BY trip_id
	`, projectID))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "service_date", Value: serviceDate},
		{Name: "trip_ids", Value: tripIDs},
	}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var row struct {
			TripID                string                 `bigquery:"trip_id"`
			VehicleID             string                 `bigquery:"vehicle_id"`
			ActualDeparture       bigquery.NullTimestamp `bigquery:"actual_departure"`
			DepartureDelaySeconds bigquery.NullInt64     `bigquery:"departure_delay_seconds"`
			DepartureConfidence   bigquery.NullString    `bigquery:"departure_confidence"`
			ObservedRunning       bool                   `bigquery:"observed_running"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		summary := schoolObservationSummary{
			VehicleID:           row.VehicleID,
			DepartureConfidence: row.DepartureConfidence.StringVal,
			ObservedRunning:     row.ObservedRunning,
		}
		if row.ActualDeparture.Valid {
			value := row.ActualDeparture.Timestamp
			summary.ActualDeparture = &value
		}
		if row.DepartureDelaySeconds.Valid {
			value := round1(float64(row.DepartureDelaySeconds.Int64) / 60)
			summary.DepartureDelayMinutes = &value
		}
		out[row.TripID] = summary
	}
	return out, nil
}

func querySchoolProbes(ctx context.Context, serviceDate civil.Date, tripIDs []string) (map[string][]probe, error) {
	out := make(map[string][]probe)
	if len(tripIDs) == 0 {
		return out, nil
	}
	q := bqClient.Query(fmt.Sprintf(`
		SELECT trip_id, observed_at, lat, lon
		FROM `+"`%s.actransit.trip_probes`"+`
		WHERE service_date = @service_date
		  AND trip_id IN UNNEST(@trip_ids)
		QUALIFY ROW_NUMBER() OVER (
		  PARTITION BY trip_id, observed_at
		  ORDER BY ingested_at DESC
		) = 1
		ORDER BY trip_id, observed_at
	`, projectID))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "service_date", Value: serviceDate},
		{Name: "trip_ids", Value: tripIDs},
	}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	for {
		var row struct {
			TripID     string    `bigquery:"trip_id"`
			ObservedAt time.Time `bigquery:"observed_at"`
			Lat        float64   `bigquery:"lat"`
			Lon        float64   `bigquery:"lon"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		out[row.TripID] = append(out[row.TripID], probe{TS: row.ObservedAt, Lat: row.Lat, Lon: row.Lon})
	}
	return out, nil
}

func detectSchoolArrival(probes []probe, stop gtfsStop) (time.Time, string, bool) {
	for i, current := range probes {
		currentDist := haversineMeters(current.Lat, current.Lon, stop.Lat, stop.Lon)
		if currentDist > schoolArrivalGeofenceMeters {
			continue
		}
		if i == 0 {
			return current.TS, arrivalConfidenceFirstSeen, true
		}
		previous := probes[i-1]
		previousDist := haversineMeters(previous.Lat, previous.Lon, stop.Lat, stop.Lon)
		if previousDist <= schoolArrivalGeofenceMeters {
			return current.TS, arrivalConfidenceFirstSeen, true
		}
		return interpolateGeofenceCrossing(
			previous.TS,
			current.TS,
			previousDist,
			currentDist,
			schoolArrivalGeofenceMeters,
		), arrivalConfidenceConfirmed, true
	}
	return time.Time{}, "", false
}

func buildSchoolStats(serviceDate civil.Date, scheduled []scheduledSchoolTrip, observations map[string]schoolObservationSummary, probes map[string][]probe, definitions []schoolDefinition) *schoolStats {
	out := &schoolStats{
		MethodologyVersion:            schoolStatsMethodologyVersion,
		ServiceDate:                   serviceDate.String(),
		GeneratedAt:                   time.Now().UTC(),
		GeofenceRadiusM:               schoolArrivalGeofenceMeters,
		DepartureLateThresholdMinutes: int(schoolDepartureLateThreshold / time.Minute),
	}
	bySchool := make(map[string][]scheduledSchoolTrip)
	for _, trip := range scheduled {
		bySchool[trip.SchoolID] = append(bySchool[trip.SchoolID], trip)
	}

	for _, definition := range definitions {
		period, ok := schoolStartPeriodFor(serviceDate, definition.StartPeriods)
		if !ok {
			continue
		}
		calendar, ok := schoolCalendarPeriodFor(serviceDate, definition.CalendarPeriods)
		if !ok {
			continue
		}
		closedDates := make([]string, len(calendar.NoSchoolDates))
		for i, date := range calendar.NoSchoolDates {
			closedDates[i] = date.String()
		}
		school := schoolDayStats{
			SchoolID:             definition.ID,
			Name:                 definition.Name,
			StartTimeLocal:       period.StartTime,
			StartTimeValidFrom:   period.ValidFrom.String(),
			StartTimeSourceURL:   period.SourceURL,
			SchoolCalendarName:   calendar.Name,
			SchoolCalendarStart:  calendar.StartDate.String(),
			SchoolCalendarEnd:    calendar.EndDate.String(),
			SchoolCalendarClosed: closedDates,
			SchoolCalendarSource: calendar.SourceURL,
		}
		if !period.ValidUntil.IsZero() {
			value := period.ValidUntil.String()
			school.StartTimeValidUntil = &value
		}

		for _, scheduledTrip := range bySchool[definition.ID] {
			observation := observations[scheduledTrip.TripID]
			trip := schoolTripStats{
				RouteID:            scheduledTrip.RouteID,
				TripID:             scheduledTrip.TripID,
				VehicleID:          observation.VehicleID,
				StopID:             scheduledTrip.StopID,
				ScheduledDeparture: scheduledTrip.ScheduledDeparture,
				ScheduledArrival:   scheduledTrip.ScheduledArrival,
				ObservedRunning:    observation.ObservedRunning || len(probes[scheduledTrip.TripID]) > 0,
				DepartureStatus:    "not_observed",
			}
			if observation.ActualDeparture != nil {
				trip.ActualDeparture = observation.ActualDeparture
				trip.DepartureConfidence = observation.DepartureConfidence
				trip.DepartureDelayMinutes = observation.DepartureDelayMinutes
				switch {
				case observation.DepartureConfidence != departureConfidenceConfirmed:
					trip.DepartureStatus = "uncertain"
				case observation.DepartureDelayMinutes != nil && *observation.DepartureDelayMinutes > float64(schoolDepartureLateThreshold/time.Minute):
					trip.DepartureStatus = "more_than_15_minutes_late"
				default:
					trip.DepartureStatus = "within_15_minutes"
				}
			}

			arrival, confidence, arrived := detectSchoolArrival(probes[scheduledTrip.TripID], scheduledTrip.Stop)
			switch {
			case arrived:
				trip.ActualArrival = &arrival
				trip.ArrivalConfidence = confidence
				minutes := round1(arrival.Sub(scheduledTrip.StartTime).Minutes())
				trip.ArrivalMinutesFromStart = &minutes
				school.ObservedArrivals++
				if arrival.After(scheduledTrip.StartTime) {
					trip.ArrivalStatus = "after_school_start"
					school.ArrivalsAfterStart++
				} else {
					trip.ArrivalStatus = "by_school_start"
				}
			case trip.ObservedRunning:
				trip.ArrivalStatus = "arrival_not_observed"
			default:
				trip.ArrivalStatus = "not_observed"
				school.TripsNotObserved++
			}
			school.Trips = append(school.Trips, trip)
		}
		school.ScheduledTrips = len(school.Trips)
		school.AllScheduledArrived = school.ScheduledTrips > 0 && school.ObservedArrivals == school.ScheduledTrips
		out.Schools = append(out.Schools, school)
	}
	return out
}

func writeSchoolStats(ctx context.Context, serviceDate civil.Date, stats *schoolStats) error {
	hasScheduledTrips := false
	for _, school := range stats.Schools {
		if school.ScheduledTrips > 0 {
			hasScheduledTrips = true
			break
		}
	}
	if !hasScheduledTrips {
		return removeSchoolStatsDate(ctx, serviceDate)
	}
	payload, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("marshal school stats: %w", err)
	}
	updateLatest, err := isAtLeastAsRecentAsSchoolLatest(ctx, serviceDate)
	if err != nil {
		updateLatest = true
	}
	if updateLatest {
		if err := writeObject(ctx, schoolStatsLatestKey, payload); err != nil {
			return fmt.Errorf("write school latest: %w", err)
		}
	}
	archiveKey := fmt.Sprintf("%s%s.json", schoolStatsPrefix, serviceDate.String())
	if err := writeObject(ctx, archiveKey, payload); err != nil {
		return fmt.Errorf("write school archive: %w", err)
	}
	return updateSchoolStatsIndex(ctx, serviceDate.String())
}

func removeSchoolStatsDate(ctx context.Context, serviceDate civil.Date) error {
	archiveKey := fmt.Sprintf("%s%s.json", schoolStatsPrefix, serviceDate.String())
	if err := gcsClient.Bucket(bucketName).Object(archiveKey).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("delete school archive: %w", err)
	}
	body, exists, err := readObject(ctx, schoolStatsLatestKey)
	if err != nil {
		return err
	}
	if exists {
		var latest schoolStats
		if json.Unmarshal(body, &latest) == nil && latest.ServiceDate == serviceDate.String() {
			if err := gcsClient.Bucket(bucketName).Object(schoolStatsLatestKey).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return fmt.Errorf("delete school latest: %w", err)
			}
		}
	}
	return removeSchoolStatsIndexDate(ctx, serviceDate.String())
}

func isAtLeastAsRecentAsSchoolLatest(ctx context.Context, candidate civil.Date) (bool, error) {
	body, exists, err := readObject(ctx, schoolStatsLatestKey)
	if err != nil || !exists {
		return !exists, err
	}
	var existing schoolStats
	if err := json.Unmarshal(body, &existing); err != nil {
		return false, err
	}
	existingDate, err := civil.ParseDate(existing.ServiceDate)
	if err != nil {
		return false, err
	}
	return !candidate.Before(existingDate), nil
}

type schoolStatsIndex struct {
	Dates     []string  `json:"dates"`
	UpdatedAt time.Time `json:"updated_at"`
}

func updateSchoolStatsIndex(ctx context.Context, date string) error {
	index := schoolStatsIndex{}
	body, exists, err := readObject(ctx, schoolStatsIndexKey)
	if err != nil {
		return err
	}
	if exists {
		_ = json.Unmarshal(body, &index)
	}
	found := false
	for _, existing := range index.Dates {
		if existing == date {
			found = true
			break
		}
	}
	if !found {
		index.Dates = append(index.Dates, date)
		sort.Sort(sort.Reverse(sort.StringSlice(index.Dates)))
	}
	index.UpdatedAt = time.Now().UTC()
	payload, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return writeObject(ctx, schoolStatsIndexKey, payload)
}

func removeSchoolStatsIndexDate(ctx context.Context, date string) error {
	index := schoolStatsIndex{}
	body, exists, err := readObject(ctx, schoolStatsIndexKey)
	if err != nil || !exists {
		return err
	}
	if err := json.Unmarshal(body, &index); err != nil {
		return err
	}
	dates := index.Dates[:0]
	for _, existing := range index.Dates {
		if existing != date {
			dates = append(dates, existing)
		}
	}
	if len(dates) == len(index.Dates) {
		return nil
	}
	index.Dates = dates
	index.UpdatedAt = time.Now().UTC()
	payload, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return writeObject(ctx, schoolStatsIndexKey, payload)
}
