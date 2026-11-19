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
	outageStart := time.Date(2026, 8, 31, 16, 5, 0, 0, time.UTC)
	outageEnd := outageStart.Add(2 * time.Minute)
	latency := func(value float64) bigquery.NullFloat64 {
		return bigquery.NullFloat64{Float64: value, Valid: true}
	}
	rows := []apiHealthAggregateRow{
		{
			Source:                 apiSourceRidership,
			IsTotal:                true,
			Requests:               100,
			SuccessfulRequests:     98,
			P50LatencyMS:           latency(125.44),
			P95LatencyMS:           latency(505.55),
			P99LatencyMS:           latency(30000.02),
			MaxLatencyMS:           latency(30001.08),
			TimeoutCount:           1,
			HTTP4xxCount:           1,
			ExpectedPolls:          bigquery.NullInt64{Int64: 100, Valid: true},
			SuccessfulPolls:        bigquery.NullInt64{Int64: 98, Valid: true},
			LongestOutageMinutes:   bigquery.NullInt64{Int64: 2, Valid: true},
			LongestOutageStartedAt: bigquery.NullTimestamp{Timestamp: outageStart, Valid: true},
			LongestOutageEndedAt:   bigquery.NullTimestamp{Timestamp: outageEnd, Valid: true},
		},
		{
			Source:             apiSourceRidership,
			HourStart:          bigquery.NullTimestamp{Timestamp: hour, Valid: true},
			Requests:           60,
			SuccessfulRequests: 59,
			P50LatencyMS:       latency(120),
			P95LatencyMS:       latency(480),
			P99LatencyMS:       latency(1000),
			MaxLatencyMS:       latency(1200),
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
	if ridership.SuccessPct != 98 || ridership.P50LatencyMS != 125.4 || ridership.P95LatencyMS != 505.6 || ridership.MaxLatencyMS != 30001.1 {
		t.Fatalf("ridership summary = %+v", ridership)
	}
	if ridership.PollAvailabilityPct != 98 || ridership.LongestOutageMinutes != 2 {
		t.Fatalf("ridership availability = %+v", ridership)
	}
	if ridership.LongestOutageStartedAt == nil || !ridership.LongestOutageStartedAt.Equal(outageStart) ||
		ridership.LongestOutageEndedAt == nil || !ridership.LongestOutageEndedAt.Equal(outageEnd) {
		t.Fatalf("ridership outage window = %v..%v", ridership.LongestOutageStartedAt, ridership.LongestOutageEndedAt)
	}
	if len(ridership.Buckets) != 1 || !ridership.Buckets[0].StartedAt.Equal(hour) {
		t.Fatalf("ridership buckets = %+v", ridership.Buckets)
	}
}

func TestAPIHealthDateWindowPreservesDST(t *testing.T) {
	start, end, err := apiHealthDateWindow(
		civil.Date{Year: 2026, Month: 11, Day: 1},
		civil.Date{Year: 2026, Month: 11, Day: 7},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := end.Sub(start), 169*time.Hour; got != want {
		t.Fatalf("window duration = %s, want %s", got, want)
	}
}

func TestAPISuccessPctKeepsVisibleFailures(t *testing.T) {
	if got, want := apiSuccessPct(10082, 10083), 99.99; got != want {
		t.Fatalf("apiSuccessPct = %v, want %v", got, want)
	}
}

func TestAssembleAPIHealthStatsWithoutRows(t *testing.T) {
	date := civil.Date{Year: 2026, Month: 8, Day: 31}
	if got := assembleAPIHealthStats(date, date, nil); got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}
