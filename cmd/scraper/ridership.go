package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	ridershipAPIURL             = "https://api.actransit.org/transit/vehicle/realtimeattributes"
	ridershipLatestObjectKey    = "ridership/latest.json"
	ridershipHistoryObjectKey   = "ridership/24h.json"
	ridershipBQTable            = "ridership_observations"
	ridershipSchemaVersion      = 1
	ridershipMethodologyVersion = 3
	ridershipHistoryPoints      = 24 * 60
	ridershipFreshnessWindow    = 5 * time.Minute
)

var ridershipStatusFactors = map[string]float64{
	"not crowded":   0.30,
	"some crowding": 0.60,
	"crowded":       0.90,
}

var ridershipTimestampLocation, ridershipTimestampLocationErr = time.LoadLocation("America/Los_Angeles")

type realtimeVehicleAttributes struct {
	VehicleID                     string     `json:"VehicleId"`
	CurrentRoute                  string     `json:"CurrentRoute"`
	LastPositionLatitude          *float64   `json:"LastPositionLatitude"`
	LastPositionLongitude         *float64   `json:"LastPositionLongitude"`
	DateTimePositionReported      *time.Time `json:"DateTimePositionReported"`
	VehicleCapacity               *int64     `json:"VehicleCapacity"`
	CurrentPassengerCount         *int64     `json:"CurrentPassengerCount"`
	EstimatedOccupancyPercentage  *int64     `json:"EstimatedOccupancyPercentage"`
	EstimatedOccupancyStatusColor string     `json:"EstimatedOccupancyStatusColor"`
	EstimatedOccupancyStatus      string     `json:"EstimatedOccupancyStatus"`
	DateTimeAPCReported           *time.Time `json:"DateTimeAPCReported"`
}

func (v *realtimeVehicleAttributes) UnmarshalJSON(data []byte) error {
	type plain realtimeVehicleAttributes
	raw := struct {
		*plain
		DateTimePositionReported json.RawMessage `json:"DateTimePositionReported"`
		DateTimeAPCReported      json.RawMessage `json:"DateTimeAPCReported"`
	}{plain: (*plain)(v)}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	positionReportedAt, err := parseRealtimeTimestamp(raw.DateTimePositionReported)
	if err != nil {
		return fmt.Errorf("DateTimePositionReported: %w", err)
	}
	apcReportedAt, err := parseRealtimeTimestamp(raw.DateTimeAPCReported)
	if err != nil {
		return fmt.Errorf("DateTimeAPCReported: %w", err)
	}
	v.DateTimePositionReported = positionReportedAt
	v.DateTimeAPCReported = apcReportedAt
	return nil
}

func parseRealtimeTimestamp(raw json.RawMessage) (*time.Time, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return &parsed, nil
	}
	if ridershipTimestampLocationErr != nil {
		return nil, ridershipTimestampLocationErr
	}
	parsed, err := time.ParseInLocation("2006-02-01T15:04:05", value, ridershipTimestampLocation)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", value, err)
	}
	return &parsed, nil
}

type ridershipVehicleSnapshot struct {
	VehicleID            string     `json:"vehicle_id"`
	RouteID              string     `json:"route_id,omitempty"`
	TripID               string     `json:"trip_id,omitempty"`
	Latitude             *float64   `json:"latitude,omitempty"`
	Longitude            *float64   `json:"longitude,omitempty"`
	PositionReportedAt   *time.Time `json:"position_reported_at,omitempty"`
	Capacity             *int64     `json:"capacity,omitempty"`
	PassengerCount       *int64     `json:"passenger_count,omitempty"`
	OccupancyPercentage  *int64     `json:"occupancy_percentage,omitempty"`
	OccupancyStatus      string     `json:"occupancy_status,omitempty"`
	OccupancyStatusColor string     `json:"occupancy_status_color,omitempty"`
	APCReportedAt        *time.Time `json:"apc_reported_at,omitempty"`
	EstimatedRiders      *int64     `json:"estimated_riders,omitempty"`
	EstimateSource       string     `json:"estimate_source,omitempty"`
}

type ridershipSummary struct {
	ObservedAt                      time.Time      `json:"observed_at"`
	ActiveVehicles                  int            `json:"active_vehicles"`
	PositionReportingVehicles       int            `json:"position_reporting_vehicles"`
	APCReportingVehicles            int            `json:"apc_reporting_vehicles"`
	PassengerCountReportingVehicles int            `json:"passenger_count_reporting_vehicles"`
	EstimatedVehicles               int            `json:"estimated_vehicles"`
	EstimatedRiders                 *int64         `json:"estimated_riders"`
	TotalCapacity                   int64          `json:"total_capacity"`
	StatusCounts                    map[string]int `json:"status_counts"`
}

type ridershipSnapshot struct {
	SchemaVersion      int                        `json:"schema_version"`
	MethodologyVersion int                        `json:"methodology_version"`
	ObservedAt         time.Time                  `json:"observed_at"`
	IngestedAt         time.Time                  `json:"ingested_at"`
	Summary            ridershipSummary           `json:"summary"`
	Vehicles           []ridershipVehicleSnapshot `json:"vehicles"`
	History            ridershipHistory           `json:"history"`
}

type ridershipHistory struct {
	SchemaVersion      int                `json:"schema_version"`
	MethodologyVersion int                `json:"methodology_version"`
	UpdatedAt          time.Time          `json:"updated_at"`
	Points             []ridershipSummary `json:"points"`
}

type ridershipObservationRow struct {
	ServiceDate                   civil.Date             `bigquery:"service_date"`
	ObservedAt                    time.Time              `bigquery:"observed_at"`
	VehicleID                     string                 `bigquery:"vehicle_id"`
	RouteID                       bigquery.NullString    `bigquery:"route_id"`
	TripID                        bigquery.NullString    `bigquery:"trip_id"`
	Latitude                      bigquery.NullFloat64   `bigquery:"latitude"`
	Longitude                     bigquery.NullFloat64   `bigquery:"longitude"`
	PositionReportedAt            bigquery.NullTimestamp `bigquery:"position_reported_at"`
	VehicleCapacity               bigquery.NullInt64     `bigquery:"vehicle_capacity"`
	CurrentPassengerCount         bigquery.NullInt64     `bigquery:"current_passenger_count"`
	EstimatedOccupancyPercentage  bigquery.NullInt64     `bigquery:"estimated_occupancy_percentage"`
	EstimatedOccupancyStatus      bigquery.NullString    `bigquery:"estimated_occupancy_status"`
	EstimatedOccupancyStatusColor bigquery.NullString    `bigquery:"estimated_occupancy_status_color"`
	APCReportedAt                 bigquery.NullTimestamp `bigquery:"apc_reported_at"`
	IngestedAt                    time.Time              `bigquery:"ingested_at"`
}

type ridershipStats struct {
	Vehicles          int
	APCReporting      int
	PassengerCounting int
	EstimatedVehicles int
	EstimatedRiders   int64
	BQRows            int
}

type vehicleTripContext struct {
	TripID     string
	ReportedAt time.Time
}

func handleScrapeRidership(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	stats, err := scrapeRidership(r.Context(), start)
	if err != nil {
		slog.Error("scrape-ridership failed", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("scrape-ridership ok",
		"duration_ms", time.Since(start).Milliseconds(),
		"vehicles", stats.Vehicles,
		"apc_reporting", stats.APCReporting,
		"passenger_count_reporting", stats.PassengerCounting,
		"estimated_vehicles", stats.EstimatedVehicles,
		"estimated_riders", stats.EstimatedRiders,
		"bq_rows", stats.BQRows,
	)
	fmt.Fprintln(w, "ok")
}

func scrapeRidership(ctx context.Context, now time.Time) (ridershipStats, error) {
	var stats ridershipStats
	token, err := ridershipToken.Get(ctx)
	if err != nil {
		return stats, fmt.Errorf("get ridership token: %w", err)
	}
	vehicles, err := fetchRidershipAttributes(ctx, token)
	if err != nil {
		return stats, err
	}

	tripContexts, err := readVehicleTripContexts(ctx)
	if err != nil {
		slog.Warn("ridership trip enrichment unavailable", "err", err)
	}

	observedAt := now.UTC().Truncate(time.Minute)
	ingestedAt := now.UTC()
	snapshot := buildRidershipSnapshot(vehicles, tripContexts, observedAt, ingestedAt)
	if err := writeRidershipRows(ctx, snapshot); err != nil {
		return stats, fmt.Errorf("insert ridership observations: %w", err)
	}
	history, err := loadRidershipHistory(ctx)
	if err != nil {
		return stats, fmt.Errorf("load ridership history: %w", err)
	}
	snapshot.History = mergeRidershipHistory(history, snapshot.Summary)

	payload, err := json.Marshal(snapshot)
	if err != nil {
		return stats, fmt.Errorf("marshal ridership snapshot: %w", err)
	}
	if err := writeObject(ctx, ridershipLatestObjectKey, payload); err != nil {
		return stats, fmt.Errorf("write ridership latest: %w", err)
	}

	stats.Vehicles = snapshot.Summary.ActiveVehicles
	stats.APCReporting = snapshot.Summary.APCReportingVehicles
	stats.PassengerCounting = snapshot.Summary.PassengerCountReportingVehicles
	stats.EstimatedVehicles = snapshot.Summary.EstimatedVehicles
	if snapshot.Summary.EstimatedRiders != nil {
		stats.EstimatedRiders = *snapshot.Summary.EstimatedRiders
	}
	stats.BQRows = len(snapshot.Vehicles)
	return stats, nil
}

func fetchRidershipAttributes(ctx context.Context, token string) ([]realtimeVehicleAttributes, error) {
	endpoint, err := url.Parse(ridershipAPIURL)
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("token", token)
	endpoint.RawQuery = query.Encode()
	body, observation, err := fetchObservedAPI(ctx, acTransitHTTPClient, apiSourceRidership, endpoint.String())
	defer func() { recordAPIRequest(ctx, observation) }()
	if err != nil {
		return nil, fmt.Errorf("fetch realtime attributes: %w", err)
	}
	var vehicles []realtimeVehicleAttributes
	if err := json.Unmarshal(body, &vehicles); err != nil {
		setAPIRequestFailure(&observation, "decode_error", err)
		return nil, fmt.Errorf("decode realtime attributes: %w", err)
	}
	if len(vehicles) == 0 {
		err := fmt.Errorf("realtime attributes returned no vehicles")
		setAPIRequestFailure(&observation, "invalid_payload", err)
		return nil, err
	}
	return vehicles, nil
}

func sanitizeHTTPError(err error) error {
	if urlErr, ok := err.(*url.Error); ok {
		return urlErr.Err
	}
	return err
}

func readVehicleTripContexts(ctx context.Context) (map[string]vehicleTripContext, error) {
	payload, exists, err := readObject(ctx, latestObjectKey)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var rawEntities []json.RawMessage
	if err := json.Unmarshal(payload, &rawEntities); err != nil {
		return nil, fmt.Errorf("parse latest vehicle feed: %w", err)
	}
	contexts := make(map[string]vehicleTripContext, len(rawEntities))
	um := protojson.UnmarshalOptions{DiscardUnknown: true}
	for _, raw := range rawEntities {
		entity := &gtfs.FeedEntity{}
		if err := um.Unmarshal(raw, entity); err != nil {
			continue
		}
		position := entity.GetVehicle()
		if position == nil || position.GetVehicle() == nil || position.GetTrip() == nil {
			continue
		}
		vehicleID := position.GetVehicle().GetId()
		tripID := position.GetTrip().GetTripId()
		if vehicleID == "" || tripID == "" || position.GetTimestamp() == 0 {
			continue
		}
		contexts[vehicleID] = vehicleTripContext{
			TripID:     tripID,
			ReportedAt: time.Unix(int64(position.GetTimestamp()), 0).UTC(),
		}
	}
	return contexts, nil
}

func buildRidershipSnapshot(vehicles []realtimeVehicleAttributes, contexts map[string]vehicleTripContext, observedAt, ingestedAt time.Time) ridershipSnapshot {
	snapshot := ridershipSnapshot{
		SchemaVersion:      ridershipSchemaVersion,
		MethodologyVersion: ridershipMethodologyVersion,
		ObservedAt:         observedAt,
		IngestedAt:         ingestedAt,
		Summary: ridershipSummary{
			ObservedAt:   observedAt,
			StatusCounts: make(map[string]int),
		},
		Vehicles: make([]ridershipVehicleSnapshot, 0, len(vehicles)),
	}
	var estimatedRiders int64
	for _, source := range vehicles {
		if source.VehicleID == "" {
			continue
		}
		vehicle := ridershipVehicleSnapshot{
			VehicleID:            source.VehicleID,
			RouteID:              source.CurrentRoute,
			Latitude:             source.LastPositionLatitude,
			Longitude:            source.LastPositionLongitude,
			PositionReportedAt:   source.DateTimePositionReported,
			Capacity:             source.VehicleCapacity,
			PassengerCount:       source.CurrentPassengerCount,
			OccupancyPercentage:  source.EstimatedOccupancyPercentage,
			OccupancyStatus:      source.EstimatedOccupancyStatus,
			OccupancyStatusColor: source.EstimatedOccupancyStatusColor,
			APCReportedAt:        source.DateTimeAPCReported,
		}
		if trip, ok := contexts[source.VehicleID]; ok && tripContextIsCurrent(trip, source.DateTimePositionReported, observedAt) {
			vehicle.TripID = trip.TripID
		}
		vehicle.EstimatedRiders, vehicle.EstimateSource = estimateVehicleRiders(source, observedAt)

		snapshot.Summary.ActiveVehicles++
		if source.VehicleCapacity != nil && *source.VehicleCapacity > 0 {
			snapshot.Summary.TotalCapacity += *source.VehicleCapacity
		}
		if source.DateTimePositionReported != nil && timestampIsFresh(*source.DateTimePositionReported, observedAt) {
			snapshot.Summary.PositionReportingVehicles++
		}
		if source.DateTimeAPCReported != nil && timestampIsFresh(*source.DateTimeAPCReported, observedAt) {
			if source.CurrentPassengerCount != nil || source.EstimatedOccupancyPercentage != nil || source.EstimatedOccupancyStatus != "" {
				snapshot.Summary.APCReportingVehicles++
			}
			if source.CurrentPassengerCount != nil {
				snapshot.Summary.PassengerCountReportingVehicles++
			}
			if source.EstimatedOccupancyStatus != "" {
				snapshot.Summary.StatusCounts[source.EstimatedOccupancyStatus]++
			}
		}
		if vehicle.EstimatedRiders != nil {
			snapshot.Summary.EstimatedVehicles++
			estimatedRiders += *vehicle.EstimatedRiders
		}
		snapshot.Vehicles = append(snapshot.Vehicles, vehicle)
	}
	if snapshot.Summary.EstimatedVehicles > 0 {
		snapshot.Summary.EstimatedRiders = &estimatedRiders
	}
	return snapshot
}

func estimateVehicleRiders(vehicle realtimeVehicleAttributes, observedAt time.Time) (*int64, string) {
	if vehicle.DateTimeAPCReported == nil || !timestampIsFresh(*vehicle.DateTimeAPCReported, observedAt) {
		return nil, ""
	}
	if vehicle.CurrentPassengerCount != nil && *vehicle.CurrentPassengerCount >= 0 {
		count := *vehicle.CurrentPassengerCount
		return &count, "passenger_count"
	}
	if vehicle.VehicleCapacity == nil || *vehicle.VehicleCapacity <= 0 {
		return nil, ""
	}
	if vehicle.EstimatedOccupancyPercentage != nil && *vehicle.EstimatedOccupancyPercentage >= 0 {
		count := int64(math.Round(float64(*vehicle.VehicleCapacity) * float64(*vehicle.EstimatedOccupancyPercentage) / 100))
		return &count, "occupancy_percentage"
	}
	factor, ok := ridershipStatusFactors[strings.ToLower(strings.TrimSpace(vehicle.EstimatedOccupancyStatus))]
	if !ok {
		return nil, ""
	}
	count := int64(math.Round(float64(*vehicle.VehicleCapacity) * factor))
	return &count, "status_band"
}

func timestampIsFresh(reportedAt, observedAt time.Time) bool {
	age := observedAt.Sub(reportedAt.UTC())
	return age >= -time.Minute && age <= ridershipFreshnessWindow
}

func tripContextIsCurrent(trip vehicleTripContext, positionReportedAt *time.Time, observedAt time.Time) bool {
	reference := observedAt
	if positionReportedAt != nil {
		reference = positionReportedAt.UTC()
	}
	delta := reference.Sub(trip.ReportedAt)
	if delta < 0 {
		delta = -delta
	}
	return delta <= ridershipFreshnessWindow
}

func writeRidershipRows(ctx context.Context, snapshot ridershipSnapshot) error {
	if ridershipBQWriter == nil || len(snapshot.Vehicles) == 0 {
		return nil
	}
	serviceDate := ridershipServiceDate(snapshot.ObservedAt)
	rows := make([]map[string]any, 0, len(snapshot.Vehicles))
	for _, vehicle := range snapshot.Vehicles {
		row := map[string]any{
			"service_date": bqDateValue(serviceDate),
			"observed_at":  snapshot.ObservedAt.UnixMicro(),
			"vehicle_id":   vehicle.VehicleID,
			"ingested_at":  snapshot.IngestedAt.UnixMicro(),
		}
		putString(row, "route_id", vehicle.RouteID)
		putString(row, "trip_id", vehicle.TripID)
		putFloat64(row, "latitude", vehicle.Latitude)
		putFloat64(row, "longitude", vehicle.Longitude)
		putTimestamp(row, "position_reported_at", vehicle.PositionReportedAt)
		putInt64(row, "vehicle_capacity", vehicle.Capacity)
		putInt64(row, "current_passenger_count", vehicle.PassengerCount)
		putInt64(row, "estimated_occupancy_percentage", vehicle.OccupancyPercentage)
		putString(row, "estimated_occupancy_status", vehicle.OccupancyStatus)
		putString(row, "estimated_occupancy_status_color", vehicle.OccupancyStatusColor)
		putTimestamp(row, "apc_reported_at", vehicle.APCReportedAt)
		rows = append(rows, row)
	}
	return ridershipBQWriter.Append(ctx, rows)
}

func putString(row map[string]any, field, value string) {
	if value != "" {
		row[field] = value
	}
}

func putInt64(row map[string]any, field string, value *int64) {
	if value != nil {
		row[field] = *value
	}
}

func putFloat64(row map[string]any, field string, value *float64) {
	if value != nil {
		row[field] = *value
	}
}

func putTimestamp(row map[string]any, field string, value *time.Time) {
	if value != nil {
		row[field] = value.UnixMicro()
	}
}

func ridershipServiceDate(observedAt time.Time) civil.Date {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	return civil.DateOf(observedAt.In(loc))
}

func loadRidershipHistory(ctx context.Context) (ridershipHistory, error) {
	payload, exists, err := readObject(ctx, ridershipLatestObjectKey)
	if err != nil {
		return ridershipHistory{}, err
	}
	if exists {
		var snapshot ridershipSnapshot
		if err := json.Unmarshal(payload, &snapshot); err != nil {
			return ridershipHistory{}, fmt.Errorf("parse ridership latest: %w", err)
		}
		if len(snapshot.History.Points) > 0 {
			return snapshot.History, nil
		}
	}

	payload, exists, err = readObject(ctx, ridershipHistoryObjectKey)
	if err != nil {
		return ridershipHistory{}, err
	}
	if !exists {
		return ridershipHistory{}, nil
	}
	var history ridershipHistory
	if err := json.Unmarshal(payload, &history); err != nil {
		return ridershipHistory{}, fmt.Errorf("parse legacy ridership history: %w", err)
	}
	return history, nil
}

func mergeRidershipHistory(history ridershipHistory, summary ridershipSummary) ridershipHistory {
	cutoff := summary.ObservedAt.Add(-24 * time.Hour)
	byMinute := make(map[time.Time]ridershipSummary, len(history.Points)+1)
	for _, point := range history.Points {
		minute := point.ObservedAt.UTC().Truncate(time.Minute)
		if minute.After(cutoff) && !minute.After(summary.ObservedAt) {
			point.ObservedAt = minute
			byMinute[minute] = point
		}
	}
	summary.ObservedAt = summary.ObservedAt.UTC().Truncate(time.Minute)
	byMinute[summary.ObservedAt] = summary
	points := make([]ridershipSummary, 0, len(byMinute))
	for _, point := range byMinute {
		points = append(points, point)
	}
	sort.Slice(points, func(i, j int) bool { return points[i].ObservedAt.Before(points[j].ObservedAt) })
	if len(points) > ridershipHistoryPoints {
		points = points[len(points)-ridershipHistoryPoints:]
	}
	return ridershipHistory{
		SchemaVersion:      ridershipSchemaVersion,
		MethodologyVersion: ridershipMethodologyVersion,
		UpdatedAt:          summary.ObservedAt,
		Points:             points,
	}
}
