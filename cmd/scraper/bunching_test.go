package main

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func TestComputeDailyBunching(t *testing.T) {
	base := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	schedule := &bunchingSchedule{
		TripMeta:     make(map[string]bunchingTripMeta),
		TripProgress: make(map[bunchingTripStopKey]int),
		ByStop:       make(map[bunchingStopKey][]bunchingScheduledArrival),
	}
	stopKey := bunchingStopKey{RouteID: "51A", DirectionID: "0", StopID: "S1"}
	actualOffsets := []time.Duration{0, 4 * time.Minute, 20 * time.Minute, 30 * time.Minute, 40 * time.Minute}
	var observations []bunchingObservation
	for i := 0; i < 5; i++ {
		tripID := "T" + string(rune('1'+i))
		scheduled := base.Add(time.Duration(i) * 10 * time.Minute)
		schedule.TripMeta[tripID] = bunchingTripMeta{RouteID: "51A", DirectionID: "0"}
		schedule.TripProgress[bunchingTripStopKey{TripID: tripID, StopSequence: 2}] = 55
		schedule.ByStop[stopKey] = append(schedule.ByStop[stopKey], bunchingScheduledArrival{
			TripID: tripID, Arrival: scheduled, Progress: 55, StopID: "S1", StopSeq: 2,
		})
		observations = append(observations, bunchingObservation{
			RouteID: "51A", TripID: tripID, StopID: "S1", StopSequence: 2,
			ScheduledArrival: scheduled, ActualArrival: base.Add(actualOffsets[i]),
		})
	}

	system, routes := computeDailyBunching(observations, schedule)
	route := routes["51A"]
	if route == nil {
		t.Fatal("route bunching missing")
	}
	if route.Status != "available" || route.HeadwayCV == nil || *route.HeadwayCV != 0.42 {
		t.Fatalf("unexpected route CV: status=%s cv=%v", route.Status, route.HeadwayCV)
	}
	if route.HeadwayN != 4 || route.ComparisonN != 4 {
		t.Fatalf("headways=%d comparisons=%d, want 4 each", route.HeadwayN, route.ComparisonN)
	}
	if route.BunchedHeadwayPct == nil || *route.BunchedHeadwayPct != 25 {
		t.Fatalf("bunched pct=%v, want 25", route.BunchedHeadwayPct)
	}
	if route.LongGapPct == nil || *route.LongGapPct != 25 {
		t.Fatalf("long-gap pct=%v, want 25", route.LongGapPct)
	}
	if route.ExpectedWaitMin == nil || *route.ExpectedWaitMin != 5.9 {
		t.Fatalf("expected wait=%v, want 5.9", route.ExpectedWaitMin)
	}
	if route.ScheduledExpectedWaitMin == nil || *route.ScheduledExpectedWaitMin != 5 {
		t.Fatalf("scheduled wait=%v, want 5.0", route.ScheduledExpectedWaitMin)
	}
	if route.SpacingPenaltyMin == nil || *route.SpacingPenaltyMin != 0.9 {
		t.Fatalf("spacing penalty=%v, want 0.9", route.SpacingPenaltyMin)
	}
	if len(route.ByHour) != 1 || route.ByHour[0].Hour != 8 {
		t.Fatalf("by_hour=%+v, want one 08:00 cell", route.ByHour)
	}
	if len(route.ByProgress) != 1 || route.ByProgress[0].ProgressPct != 55 {
		t.Fatalf("by_progress=%+v, want one 55%% cell", route.ByProgress)
	}
	if system.HeadwayN != route.HeadwayN || system.HeadwayCV == nil || *system.HeadwayCV != *route.HeadwayCV {
		t.Fatalf("one-route system should equal route: system=%+v route=%+v", system.bunchingMetrics, route.bunchingMetrics)
	}
}

func TestBunchingCVRequiresThreeHeadwaysInCell(t *testing.T) {
	acc := bunchingAccumulator{}
	acc.addObservedCell([]bunchingGap{{Seconds: 300}, {Seconds: 900}})
	metrics := acc.metrics()
	if metrics.Status != "insufficient_data" || metrics.HeadwayCV != nil {
		t.Fatalf("status=%s cv=%v, want insufficient with nil CV", metrics.Status, metrics.HeadwayCV)
	}
	if metrics.MeanHeadwayMin == nil || *metrics.MeanHeadwayMin != 10 {
		t.Fatalf("mean=%v, want 10", metrics.MeanHeadwayMin)
	}
}

func TestAggregateBunchingStatsMarksPartialCoverage(t *testing.T) {
	acc := bunchingAccumulator{}
	acc.addObservedCell([]bunchingGap{{Seconds: 300}, {Seconds: 600}, {Seconds: 900}})
	one := finalizeBunchingStats(acc, nil, nil, time.Unix(1, 0), 1, 1)
	two := finalizeBunchingStats(acc, nil, nil, time.Unix(2, 0), 1, 1)

	weekly := aggregateBunchingStats([]*bunchingStats{one, two, nil}, 7)
	if weekly == nil {
		t.Fatal("weekly bunching missing")
	}
	if weekly.Status != "partial" || weekly.DaysAvailable != 2 || weekly.MissingDays != 5 {
		t.Fatalf("status=%s available=%d missing=%d", weekly.Status, weekly.DaysAvailable, weekly.MissingDays)
	}
	if weekly.HeadwayN != 6 || weekly.Aggregation.CVWeight != 6 {
		t.Fatalf("headways=%d cv_weight=%d, want 6", weekly.HeadwayN, weekly.Aggregation.CVWeight)
	}
}

func TestLoadBunchingSchedule(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))
	date := civil.Date{Year: 2026, Month: 4, Day: 28}
	services, err := loadActiveServices(zr, date)
	if err != nil {
		t.Fatalf("active services: %v", err)
	}
	schedule, err := loadBunchingSchedule(zr, date, services)
	if err != nil {
		t.Fatalf("load schedule: %v", err)
	}
	if got := schedule.TripMeta["T1"]; got.RouteID != "R1" || got.DirectionID != "0" {
		t.Fatalf("T1 metadata=%+v", got)
	}
	if got := schedule.TripProgress[bunchingTripStopKey{TripID: "T1", StopSequence: 2}]; got != 55 {
		t.Fatalf("T1 stop 2 progress=%d, want 55", got)
	}
	key := bunchingStopKey{RouteID: "R1", DirectionID: "0", StopID: "S2"}
	if len(schedule.ByStop[key]) != 2 {
		t.Fatalf("R1 direction 0 S2 arrivals=%d, want 2", len(schedule.ByStop[key]))
	}
}

func TestWeekEndSaturdayForDate(t *testing.T) {
	monday := civil.Date{Year: 2026, Month: 8, Day: 24}
	if got := weekEndSaturdayForDate(monday); got.String() != "2026-08-29" {
		t.Fatalf("Monday week end=%s", got)
	}
	saturday := civil.Date{Year: 2026, Month: 8, Day: 29}
	if got := weekEndSaturdayForDate(saturday); got != saturday {
		t.Fatalf("Saturday week end=%s", got)
	}
}
