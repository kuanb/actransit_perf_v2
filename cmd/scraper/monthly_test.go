package main

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func TestAggregateMonthlyStatsUsesCalendarMonthWeekSegments(t *testing.T) {
	month := civil.Date{Year: 2026, Month: 8, Day: 1}
	dailies := make([]*dailyStats, 31)
	for i := 0; i < 2; i++ {
		kpi := emptyAgencyKPIStats()
		kpi.ServiceOperated.ScheduledTrips = 10
		kpi.ServiceOperated.OperatedTrips = 8 + i
		kpi.OnTimePerformance.OnTimeTimepoints = int64(6 + i)
		kpi.OnTimePerformance.OperatedTimepoints = int64(8 + i)
		kpi.OnTimePerformance.ScheduledTimepoints = 10
		finalizeAgencyKPIStats(&kpi)
		dailies[i] = &dailyStats{
			AgencyKPI: kpi,
			Routes:    []routeStats{{RouteID: "R1", AgencyKPI: kpi, Color: "112233", TextColor: "FFFFFF"}},
		}
	}
	generatedAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	got := aggregateMonthlyStats(month, dailies, generatedAt)
	if got.Status != "partial" || got.DaysExpected != 31 || got.DaysAvailable != 2 {
		t.Fatalf("month completeness = %+v", got)
	}
	if len(got.Weeks) != 6 {
		t.Fatalf("weeks = %d, want 6", len(got.Weeks))
	}
	if got.Weeks[0].PeriodStart != "2026-08-01" || got.Weeks[0].PeriodEnd != "2026-08-01" {
		t.Fatalf("first week = %+v", got.Weeks[0])
	}
	if got.Weeks[1].PeriodStart != "2026-08-02" || got.Weeks[1].PeriodEnd != "2026-08-08" {
		t.Fatalf("second week = %+v", got.Weeks[1])
	}
	if got.Weeks[5].PeriodStart != "2026-08-30" || got.Weeks[5].PeriodEnd != "2026-08-31" {
		t.Fatalf("last week = %+v", got.Weeks[5])
	}
	if got.AgencyKPI.ServiceOperated.OperatedTrips != 17 || got.AgencyKPI.ServiceOperated.ScheduledTrips != 20 {
		t.Fatalf("monthly raw counts = %+v", got.AgencyKPI)
	}
	if len(got.Routes) != 1 || got.Routes[0].AgencyKPI.ServiceOperated.OperatedTrips != 17 {
		t.Fatalf("route aggregation = %+v", got.Routes)
	}
}

func TestDailyKPIPercentageRange(t *testing.T) {
	values := make([]agencyKPIStats, 2)
	for i := range values {
		values[i] = emptyAgencyKPIStats()
		values[i].ServiceOperated.ScheduledTrips = 10
		values[i].ServiceOperated.OperatedTrips = 8 + i
		values[i].OnTimePerformance.OnTimeTimepoints = int64(6 + i)
		values[i].OnTimePerformance.OperatedTimepoints = int64(8 + i)
		values[i].OnTimePerformance.ScheduledTimepoints = 10
		finalizeAgencyKPIStats(&values[i])
	}

	got := dailyKPIPercentageRange(values)
	if *got.ServiceOperated.MinPct != 80 || *got.ServiceOperated.MaxPct != 90 {
		t.Fatalf("Service Operated range = %+v", got.ServiceOperated)
	}
	if *got.OTPOfOperated.MinPct != 75 || *got.OTPOfOperated.MaxPct != 77.8 {
		t.Fatalf("operated OTP range = %+v", got.OTPOfOperated)
	}
	if *got.OTPOfScheduled.MinPct != 60 || *got.OTPOfScheduled.MaxPct != 70 {
		t.Fatalf("scheduled OTP range = %+v", got.OTPOfScheduled)
	}
}

func TestDefaultMonthlyStatsMonth(t *testing.T) {
	pt, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	got := defaultMonthlyStatsMonth(time.Date(2026, 1, 15, 12, 0, 0, 0, pt))
	want := civil.Date{Year: 2025, Month: 12, Day: 1}
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestPreviousPTDate(t *testing.T) {
	pt, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	got := previousPTDate(time.Date(2026, 8, 29, 7, 0, 0, 0, pt))
	want := civil.Date{Year: 2026, Month: 8, Day: 28}
	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}
