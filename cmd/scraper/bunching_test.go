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
	if route.Status != "insufficient_data" || route.HeadwayCV != nil || route.Eligibility.Reason != "not_enough_comparable_headways" {
		t.Fatalf("unexpected route eligibility: status=%s cv=%v eligibility=%+v", route.Status, route.HeadwayCV, route.Eligibility)
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
	if route.EvenSpacingWaitMin == nil || *route.EvenSpacingWaitMin != 5 {
		t.Fatalf("even-spacing wait=%v, want 5.0", route.EvenSpacingWaitMin)
	}
	if len(route.ByHour) != 0 {
		t.Fatalf("route by_hour=%+v, want compact route payload", route.ByHour)
	}
	if len(system.ByHour) != 1 || system.ByHour[0].Hour != 8 || system.ByHour[0].HeadwayCV == nil || *system.ByHour[0].HeadwayCV != 0.42 {
		t.Fatalf("system by_hour=%+v, want one 08:00 cell with CV 0.42", system.ByHour)
	}
	if len(route.ByProgress) != 1 || route.ByProgress[0].ProgressPct != 55 {
		t.Fatalf("by_progress=%+v, want one 55%% cell", route.ByProgress)
	}
	if system.HeadwayN != route.HeadwayN || system.Status != route.Status {
		t.Fatalf("one-route system should equal route: system=%+v route=%+v", system.bunchingMetrics, route.bunchingMetrics)
	}
}

func TestComputeDailyBunchingSeparatesDirectionsAtSharedStop(t *testing.T) {
	base := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	schedule := &bunchingSchedule{
		TripMeta:     make(map[string]bunchingTripMeta),
		TripProgress: make(map[bunchingTripStopKey]int),
		ByStop:       make(map[bunchingStopKey][]bunchingScheduledArrival),
	}
	var observations []bunchingObservation
	for direction, offset := range []time.Duration{0, 5 * time.Minute} {
		key := bunchingStopKey{RouteID: "R1", DirectionID: string(rune('0' + direction)), StopID: "TERMINAL"}
		for i := 0; i < 2; i++ {
			tripID := key.DirectionID + string(rune('A'+i))
			arrival := base.Add(offset + time.Duration(i)*10*time.Minute)
			schedule.TripMeta[tripID] = bunchingTripMeta{RouteID: "R1", DirectionID: key.DirectionID}
			schedule.TripProgress[bunchingTripStopKey{TripID: tripID, StopSequence: 1}] = 10
			schedule.ByStop[key] = append(schedule.ByStop[key], bunchingScheduledArrival{
				TripID: tripID, Arrival: arrival, Progress: 10, StopID: "TERMINAL", StopSeq: 1,
			})
			observations = append(observations, bunchingObservation{
				RouteID: "R1", TripID: tripID, StopID: "TERMINAL", StopSequence: 1,
				ScheduledArrival: arrival, ActualArrival: arrival,
			})
		}
	}

	_, routes := computeDailyBunching(observations, schedule)
	route := routes["R1"]
	if route == nil {
		t.Fatal("route bunching missing")
	}
	if route.HeadwayN != 2 {
		t.Fatalf("headways=%d, want one 10-minute headway per direction", route.HeadwayN)
	}
	if route.Aggregation.ObservedHeadwaySeconds != 20*60 {
		t.Fatalf("observed headway seconds=%.0f, want 1200", route.Aggregation.ObservedHeadwaySeconds)
	}
}

func TestBunchingCVRequiresTwoHeadwaysInCell(t *testing.T) {
	acc := bunchingAccumulator{}
	acc.addObservedCell([]bunchingGap{{Seconds: 600}})
	metrics := acc.metrics(bunchingMinCellHeadways, 0)
	if metrics.Status != "insufficient_data" || metrics.HeadwayCV != nil {
		t.Fatalf("status=%s cv=%v, want insufficient with nil CV", metrics.Status, metrics.HeadwayCV)
	}
	if metrics.MeanHeadwayMin != nil {
		t.Fatalf("mean=%v, want nil without a comparable cell", metrics.MeanHeadwayMin)
	}
}

func TestAggregateBunchingStatsMarksPartialCoverage(t *testing.T) {
	acc := bunchingAccumulator{}
	gaps := make([]bunchingGap, 60)
	for i := range gaps {
		gaps[i] = bunchingGap{Seconds: 300 + float64(i%3)*300, ScheduledSeconds: 600}
		acc.addScheduledHeadway(600)
	}
	acc.addObservedCell(gaps)
	one := finalizeBunchingStats(acc, nil, nil, time.Unix(1, 0), 1, 1)
	two := finalizeBunchingStats(acc, nil, nil, time.Unix(2, 0), 1, 1)

	weekly := aggregateBunchingStats([]*bunchingStats{one, two, nil}, 7)
	if weekly == nil {
		t.Fatal("weekly bunching missing")
	}
	if weekly.Status != "partial" || weekly.DaysAvailable != 2 || weekly.MissingDays != 5 {
		t.Fatalf("status=%s available=%d missing=%d", weekly.Status, weekly.DaysAvailable, weekly.MissingDays)
	}
	if weekly.HeadwayN != 120 || weekly.Aggregation.CVWeight != 120 {
		t.Fatalf("headways=%d cv_weight=%d, want 120", weekly.HeadwayN, weekly.Aggregation.CVWeight)
	}
}

func TestBunchingTooLowFrequency(t *testing.T) {
	acc := bunchingAccumulator{}
	acc.addScheduledHeadway(41 * 60)
	stats := finalizeBunchingStats(acc, nil, nil, time.Unix(1, 0), 1, 1)
	if stats.Status != "too_low_frequency" || stats.HeadwayCV != nil {
		t.Fatalf("status=%s cv=%v", stats.Status, stats.HeadwayCV)
	}
	if stats.Eligibility.Eligible || stats.Eligibility.Reason != "too_low_frequency" {
		t.Fatalf("eligibility=%+v", stats.Eligibility)
	}
	boundary := bunchingAccumulator{}
	boundary.addScheduledHeadway(40 * 60)
	if boundary.scheduledHeadwayN != 1 {
		t.Fatalf("40-minute scheduled window should remain frequency eligible")
	}
}

func TestSpacingPenaltyUsesSameRealizedFrequency(t *testing.T) {
	acc := bunchingAccumulator{}
	acc.addObservedCell([]bunchingGap{
		{Seconds: 240, ScheduledSeconds: 180},
		{Seconds: 960, ScheduledSeconds: 180},
		{Seconds: 600, ScheduledSeconds: 180},
		{Seconds: 600, ScheduledSeconds: 180},
	})
	for i := 0; i < 4; i++ {
		acc.addScheduledHeadway(180)
	}
	metrics := acc.metrics(bunchingMinCellHeadways, 0)
	if metrics.ExpectedWaitMin == nil || *metrics.ExpectedWaitMin != 5.9 || metrics.EvenSpacingWaitMin == nil || *metrics.EvenSpacingWaitMin != 5 {
		t.Fatalf("observed=%v even=%v", metrics.ExpectedWaitMin, metrics.EvenSpacingWaitMin)
	}
	if metrics.ScheduledExpectedWaitMin == nil || *metrics.ScheduledExpectedWaitMin != 1.5 {
		t.Fatalf("scheduled wait=%v, want 1.5", metrics.ScheduledExpectedWaitMin)
	}
	if metrics.SpacingPenaltyMin == nil || *metrics.SpacingPenaltyMin != 0.9 {
		t.Fatalf("spacing penalty=%v, want 0.9", metrics.SpacingPenaltyMin)
	}
}

func TestBunchingRejectsWeakCoverage(t *testing.T) {
	acc := bunchingAccumulator{
		headwayN:             2000,
		scheduledHeadwayN:    2000,
		allScheduledHeadwayN: 2000,
		aggregation:          bunchingAggregation{CVWeight: 100, CVWeightedSum: 30},
	}
	stats := finalizeBunchingStats(acc, nil, nil, time.Unix(1, 0), 1, 1)
	if stats.Status != "insufficient_data" || stats.Eligibility.Reason != "low_comparable_headway_coverage" {
		t.Fatalf("status=%s eligibility=%+v", stats.Status, stats.Eligibility)
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
