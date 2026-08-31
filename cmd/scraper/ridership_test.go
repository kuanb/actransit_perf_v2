package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRealtimeVehicleAttributesDecode(t *testing.T) {
	var vehicles []realtimeVehicleAttributes
	payload := `[{"VehicleId":"1001","CurrentRoute":"51A","LastPositionLatitude":37.8,"LastPositionLongitude":-122.2,"DateTimePositionReported":"2026-08-31T15:06:24.297247-07:00","VehicleCapacity":50,"CurrentPassengerCount":null,"EstimatedOccupancyPercentage":null,"EstimatedOccupancyStatusColor":"#45A17C","EstimatedOccupancyStatus":"Not Crowded","DateTimeAPCReported":"2026-08-31T15:06:24.297247-07:00"}]`
	if err := json.Unmarshal([]byte(payload), &vehicles); err != nil {
		t.Fatal(err)
	}
	if len(vehicles) != 1 || vehicles[0].VehicleID != "1001" || vehicles[0].CurrentPassengerCount != nil {
		t.Fatalf("decoded vehicles = %+v", vehicles)
	}
	if vehicles[0].DateTimeAPCReported == nil || vehicles[0].DateTimeAPCReported.UTC().Hour() != 22 {
		t.Fatalf("APC timestamp = %v", vehicles[0].DateTimeAPCReported)
	}
}

func TestEstimateVehicleRiders(t *testing.T) {
	now := time.Date(2026, 8, 31, 22, 0, 0, 0, time.UTC)
	fresh := now.Add(-30 * time.Second)
	stale := now.Add(-6 * time.Minute)
	capacity := int64(50)
	count := int64(17)
	pct := int64(40)

	tests := []struct {
		name   string
		input  realtimeVehicleAttributes
		want   *int64
		source string
	}{
		{
			name: "passenger count wins",
			input: realtimeVehicleAttributes{DateTimeAPCReported: &fresh, VehicleCapacity: &capacity,
				CurrentPassengerCount: &count, EstimatedOccupancyPercentage: &pct, EstimatedOccupancyStatus: "Crowded"},
			want: &count, source: "passenger_count",
		},
		{
			name: "percentage",
			input: realtimeVehicleAttributes{DateTimeAPCReported: &fresh, VehicleCapacity: &capacity,
				EstimatedOccupancyPercentage: &pct},
			want: ptrInt64(20), source: "occupancy_percentage",
		},
		{
			name: "status band",
			input: realtimeVehicleAttributes{DateTimeAPCReported: &fresh, VehicleCapacity: &capacity,
				EstimatedOccupancyStatus: "Some Crowding"},
			want: ptrInt64(38), source: "status_band",
		},
		{
			name: "stale",
			input: realtimeVehicleAttributes{DateTimeAPCReported: &stale, VehicleCapacity: &capacity,
				CurrentPassengerCount: &count},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, source := estimateVehicleRiders(test.input, now)
			if source != test.source {
				t.Fatalf("source = %q, want %q", source, test.source)
			}
			if test.want == nil {
				if got != nil {
					t.Fatalf("got %d, want nil", *got)
				}
				return
			}
			if got == nil || *got != *test.want {
				t.Fatalf("got %v, want %d", got, *test.want)
			}
		})
	}
}

func TestBuildRidershipSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 31, 22, 0, 0, 0, time.UTC)
	fresh := now.Add(-30 * time.Second)
	capacity := int64(50)
	vehicles := []realtimeVehicleAttributes{
		{VehicleID: "1001", CurrentRoute: "51A", VehicleCapacity: &capacity, DateTimeAPCReported: &fresh, DateTimePositionReported: &fresh, EstimatedOccupancyStatus: "Not Crowded"},
		{VehicleID: "1002", CurrentRoute: "1T", VehicleCapacity: &capacity, DateTimeAPCReported: &fresh, DateTimePositionReported: &fresh, EstimatedOccupancyStatus: "Crowded"},
	}
	contexts := map[string]vehicleTripContext{
		"1001": {TripID: "trip-1", ReportedAt: fresh},
	}

	snapshot := buildRidershipSnapshot(vehicles, contexts, now, now.Add(10*time.Second))
	if snapshot.Summary.ActiveVehicles != 2 || snapshot.Summary.APCReportingVehicles != 2 {
		t.Fatalf("summary counts = %+v", snapshot.Summary)
	}
	if snapshot.Summary.EstimatedRiders == nil || *snapshot.Summary.EstimatedRiders != 68 {
		t.Fatalf("estimated riders = %v, want 68", snapshot.Summary.EstimatedRiders)
	}
	if snapshot.Vehicles[0].TripID != "trip-1" {
		t.Fatalf("trip id = %q, want trip-1", snapshot.Vehicles[0].TripID)
	}
}

func TestMergeRidershipHistory(t *testing.T) {
	now := time.Date(2026, 8, 31, 22, 0, 0, 0, time.UTC)
	old := now.Add(-24 * time.Hour)
	previous := now.Add(-time.Minute)
	history := ridershipHistory{Points: []ridershipSummary{
		{ObservedAt: old, ActiveVehicles: 1},
		{ObservedAt: previous, ActiveVehicles: 2},
		{ObservedAt: now, ActiveVehicles: 3},
	}}
	merged := mergeRidershipHistory(history, ridershipSummary{ObservedAt: now.Add(20 * time.Second), ActiveVehicles: 4})
	if len(merged.Points) != 2 {
		t.Fatalf("points = %d, want 2", len(merged.Points))
	}
	if merged.Points[1].ActiveVehicles != 4 {
		t.Fatalf("current point was not replaced: %+v", merged.Points[1])
	}
}

func ptrInt64(value int64) *int64 { return &value }
