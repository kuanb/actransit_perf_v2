package main

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

func TestAssembleAPIHealthStats(t *testing.T) {
	date := civil.Date{Year: 2026, Month: 8, Day: 31}
	hour := time.Date(2026, 9, 1, 4, 0, 0, 0, time.UTC)
	latency := func(value float64) bigquery.NullFloat64 {
		return bigquery.NullFloat64{Float64: value, Valid: true}
	}
	rows := []apiHealthAggregateRow{
		{
			Source:             apiSourceRidership,
			IsTotal:            true,
			Requests:           100,
			SuccessfulRequests: 98,
			P50LatencyMS:       latency(125.44),
			P95LatencyMS:       latency(505.55),
			P99LatencyMS:       latency(30000.02),
			TimeoutCount:       1,
			HTTP4xxCount:       1,
		},
		{
			Source:             apiSourceRidership,
			HourStart:          bigquery.NullTimestamp{Timestamp: hour, Valid: true},
			Requests:           60,
			SuccessfulRequests: 59,
			P50LatencyMS:       latency(120),
			P95LatencyMS:       latency(480),
			P99LatencyMS:       latency(1000),
			HTTP4xxCount:       1,
		},
		{
			Source:             apiSourceVehicleLocations,
			IsTotal:            true,
			Requests:           100,
			SuccessfulRequests: 100,
			P50LatencyMS:       latency(210),
			P95LatencyMS:       latency(450),
			P99LatencyMS:       latency(600),
		},
	}

	got := assembleAPIHealthStats(date, date, rows)
	if got == nil {
		t.Fatal("assembleAPIHealthStats returned nil")
	}
	if got.PeriodStart != "2026-08-31" || got.PeriodEnd != "2026-08-31" {
		t.Fatalf("period = %s..%s", got.PeriodStart, got.PeriodEnd)
	}
	if len(got.Sources) != 2 {
		t.Fatalf("sources = %d, want 2", len(got.Sources))
	}
	if got.Sources[0].Source != apiSourceVehicleLocations || got.Sources[1].Source != apiSourceRidership {
		t.Fatalf("source order = %q, %q", got.Sources[0].Source, got.Sources[1].Source)
	}
	ridership := got.Sources[1]
	if ridership.SuccessPct != 98 || ridership.P50LatencyMS != 125.4 || ridership.P95LatencyMS != 505.6 {
		t.Fatalf("ridership summary = %+v", ridership)
	}
	if len(ridership.Buckets) != 1 || !ridership.Buckets[0].StartedAt.Equal(hour) {
		t.Fatalf("ridership buckets = %+v", ridership.Buckets)
	}
}

func TestAssembleAPIHealthStatsWithoutRows(t *testing.T) {
	date := civil.Date{Year: 2026, Month: 8, Day: 31}
	if got := assembleAPIHealthStats(date, date, nil); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}
