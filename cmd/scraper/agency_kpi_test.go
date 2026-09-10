package main

import (
	"testing"

	"cloud.google.com/go/civil"
)

func TestAgencyKPIOnTimeWindow(t *testing.T) {
	if agencyKPIEarlySeconds != -60 || agencyKPILateSeconds != 300 {
		t.Fatalf("window = [%d, %d], want [-60, 300]", agencyKPIEarlySeconds, agencyKPILateSeconds)
	}
}

func TestLoadKPITimepointPlan(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))
	tripRoutes := map[string]string{"T1": "R1", "T2": "R1", "T3": "R2"}
	plan, err := loadKPITimepointPlan(zr, tripRoutes)
	if err != nil {
		t.Fatalf("loadKPITimepointPlan: %v", err)
	}
	if len(plan.ByTrip["T1"]) != 1 || plan.ByTrip["T1"][0].Key != (scheduledStopKey{"T1", 2}) {
		t.Fatalf("T1 timepoints = %+v, want only interior stop 2", plan.ByTrip["T1"])
	}
	if len(plan.ByTrip["T2"]) != 1 || plan.ByTrip["T2"][0].Key != (scheduledStopKey{"T2", 3}) {
		t.Fatalf("T2 timepoints = %+v, want pickup-eligible interior stop 3", plan.ByTrip["T2"])
	}
	if len(plan.ByTrip["T3"]) != 0 {
		t.Fatalf("T3 timepoints = %+v, want none because it has no interior stop", plan.ByTrip["T3"])
	}
	if plan.TripStartSeconds["T1"] != 8*60*60 || plan.ByTrip["T1"][0].ScheduledSeconds != 8*60*60+10*60 {
		t.Fatalf("T1 schedule = %+v", plan)
	}
}

func TestBuildAgencyKPIStats(t *testing.T) {
	scheduled := map[string]string{"T1": "R1", "T2": "R1", "T3": "R2"}
	stopPlan := &scheduledStopPlan{
		LastBoardingSequence: map[string]int64{"T1": 2, "T2": 3, "T3": 2},
	}
	timepoints := &kpiTimepointPlan{
		ByTrip: map[string][]kpiScheduledTimepoint{
			"T1": {{Key: scheduledStopKey{"T1", 2}, ScheduledSeconds: 8 * 60 * 60}},
			"T2": {{Key: scheduledStopKey{"T2", 3}, ScheduledSeconds: 15 * 60 * 60}},
			"T3": {{Key: scheduledStopKey{"T3", 2}, ScheduledSeconds: 25 * 60 * 60}},
		},
		TripStartSeconds: map[string]int{
			"T1": 8 * 60 * 60,
			"T2": 9 * 60 * 60,
			"T3": 25 * 60 * 60,
		},
	}
	observed := map[string]observedTripProgress{
		"T1":          {LastStopSequence: 2, HasStopSequence: true},
		"T2":          {LastStopSequence: 2, HasStopSequence: true},
		"UNSCHEDULED": {LastStopSequence: 10, HasStopSequence: true},
	}
	onTime := map[scheduledStopKey]struct{}{{"T1", 2}: {}, {"T3", 2}: {}}

	system, routes, breakdowns, err := buildAgencyKPIStats(
		scheduled,
		stopPlan,
		timepoints,
		observed,
		map[string]int64{},
		onTime,
		"weekday",
	)
	if err != nil {
		t.Fatalf("buildAgencyKPIStats: %v", err)
	}
	if system.ServiceOperated.ScheduledTrips != 3 || system.ServiceOperated.OperatedTrips != 2 {
		t.Fatalf("system service operated counts = %+v", system.ServiceOperated)
	}
	if system.ServiceOperated.PartialTrips != 1 || *system.ServiceOperated.PartialOfRunPct != 50 {
		t.Fatalf("system partial counts = %+v", system.ServiceOperated)
	}
	if *system.ServiceOperated.OperatedPct != 66.7 {
		t.Fatalf("operated pct = %v, want 66.7", *system.ServiceOperated.OperatedPct)
	}
	if system.OnTimePerformance.OnTimeTimepoints != 1 || system.OnTimePerformance.OperatedTimepoints != 2 || system.OnTimePerformance.ScheduledTimepoints != 3 {
		t.Fatalf("system OTP counts = %+v", system.OnTimePerformance)
	}
	if *system.OnTimePerformance.OfOperatedPct != 50 || *system.OnTimePerformance.OfScheduledPct != 33.3 {
		t.Fatalf("system OTP percentages = %+v", system.OnTimePerformance)
	}
	if routes["R1"].ServiceOperated.OperatedTrips != 2 || routes["R2"].ServiceOperated.OperatedTrips != 0 {
		t.Fatalf("route counts = %+v", routes)
	}
	r1Morning := breakdowns["R1"]["weekday"]["morning"]
	if r1Morning.ServiceOperated.ScheduledTrips != 2 || r1Morning.ServiceOperated.PartialTrips != 1 {
		t.Fatalf("R1 morning Service Operated = %+v", r1Morning.ServiceOperated)
	}
	if r1Morning.OnTimePerformance.OnTimeTimepoints != 1 || r1Morning.OnTimePerformance.ScheduledTimepoints != 1 {
		t.Fatalf("R1 morning OTP = %+v", r1Morning.OnTimePerformance)
	}
	r1Afternoon := breakdowns["R1"]["weekday"]["afternoon"]
	if r1Afternoon.OnTimePerformance.OnTimeTimepoints != 0 || r1Afternoon.OnTimePerformance.ScheduledTimepoints != 1 {
		t.Fatalf("R1 afternoon OTP = %+v", r1Afternoon.OnTimePerformance)
	}
	r2Owl := breakdowns["R2"]["weekday"]["owl"]
	if r2Owl.ServiceOperated.ScheduledTrips != 1 || r2Owl.OnTimePerformance.ScheduledTimepoints != 1 {
		t.Fatalf("R2 owl = %+v", r2Owl)
	}
}

func TestAggregateAgencyKPIStatsUsesRawCounts(t *testing.T) {
	one := emptyAgencyKPIStats()
	one.ServiceOperated.ScheduledTrips = 1
	one.ServiceOperated.OperatedTrips = 1
	one.OnTimePerformance.OnTimeTimepoints = 1
	one.OnTimePerformance.OperatedTimepoints = 1
	one.OnTimePerformance.ScheduledTimepoints = 1
	finalizeAgencyKPIStats(&one)
	two := emptyAgencyKPIStats()
	two.ServiceOperated.ScheduledTrips = 9
	two.ServiceOperated.OperatedTrips = 4
	two.OnTimePerformance.OnTimeTimepoints = 4
	two.OnTimePerformance.OperatedTimepoints = 9
	two.OnTimePerformance.ScheduledTimepoints = 12
	finalizeAgencyKPIStats(&two)

	got := aggregateAgencyKPIStats([]agencyKPIStats{one, two})
	if *got.ServiceOperated.OperatedPct != 50 {
		t.Fatalf("service operated = %v, want 5/10 = 50", *got.ServiceOperated.OperatedPct)
	}
	if *got.OnTimePerformance.OfOperatedPct != 50 || *got.OnTimePerformance.OfScheduledPct != 38.5 {
		t.Fatalf("OTP = %+v, want 5/10 and 5/13", got.OnTimePerformance)
	}
}

func TestBuildAgencyKPIStatsUsesArrivalProgressWhenProbeSequenceIsMissing(t *testing.T) {
	system, _, _, err := buildAgencyKPIStats(
		map[string]string{"T1": "R1"},
		&scheduledStopPlan{LastBoardingSequence: map[string]int64{"T1": 4}},
		&kpiTimepointPlan{
			ByTrip:           map[string][]kpiScheduledTimepoint{},
			TripStartSeconds: map[string]int{"T1": 8 * 60 * 60},
		},
		map[string]observedTripProgress{"T1": {}},
		map[string]int64{"T1": 4},
		map[scheduledStopKey]struct{}{},
		"weekday",
	)
	if err != nil {
		t.Fatal(err)
	}
	if system.ServiceOperated.PartialTrips != 0 {
		t.Fatalf("partial trips = %d, want 0", system.ServiceOperated.PartialTrips)
	}
}

func TestAgencyKPITimePeriods(t *testing.T) {
	cases := map[int]string{
		0:                "owl",
		5*60*60 + 59*60:  "owl",
		6 * 60 * 60:      "morning",
		10 * 60 * 60:     "midday",
		15 * 60 * 60:     "afternoon",
		19 * 60 * 60:     "evening",
		24 * 60 * 60:     "owl",
		25*60*60 + 10*60: "owl",
	}
	for seconds, want := range cases {
		if got := agencyKPITimePeriod(seconds); got != want {
			t.Errorf("agencyKPITimePeriod(%d) = %q, want %q", seconds, got, want)
		}
	}
}

func TestParseGTFSTimeSeconds(t *testing.T) {
	got, ok, err := parseGTFSTimeSeconds("25:10:30")
	if err != nil || !ok || got != 25*60*60+10*60+30 {
		t.Fatalf("parse 25:10:30 = %d, %v, %v", got, ok, err)
	}
	if _, ok, err := parseGTFSTimeSeconds(""); err != nil || ok {
		t.Fatalf("parse empty = ok %v, err %v", ok, err)
	}
	if _, _, err := parseGTFSTimeSeconds("08:60:00"); err == nil {
		t.Fatal("invalid minute should fail")
	}
	if _, _, err := parseGTFSTimeSeconds("08:10:00 UTC"); err == nil {
		t.Fatal("trailing input should fail")
	}
}

func TestAgencyKPIDayType(t *testing.T) {
	if got := agencyKPIDayType(civil.Date{Year: 2026, Month: 9, Day: 7}); got != "weekday" {
		t.Fatalf("Monday = %q", got)
	}
	if got := agencyKPIDayType(civil.Date{Year: 2026, Month: 9, Day: 6}); got != "weekend" {
		t.Fatalf("Sunday = %q", got)
	}
}

func TestAggregateAgencyKPIBreakdownsUsesRawCounts(t *testing.T) {
	weekday := emptyAgencyKPIStats()
	weekday.ServiceOperated.ScheduledTrips = 10
	weekday.ServiceOperated.OperatedTrips = 8
	weekday.OnTimePerformance.OnTimeTimepoints = 6
	weekday.OnTimePerformance.OperatedTimepoints = 8
	weekday.OnTimePerformance.ScheduledTimepoints = 10
	finalizeAgencyKPIStats(&weekday)
	weekend := emptyAgencyKPIStats()
	weekend.ServiceOperated.ScheduledTrips = 2
	weekend.ServiceOperated.OperatedTrips = 1
	weekend.OnTimePerformance.OnTimeTimepoints = 1
	weekend.OnTimePerformance.OperatedTimepoints = 2
	weekend.OnTimePerformance.ScheduledTimepoints = 3
	finalizeAgencyKPIStats(&weekend)

	got := aggregateAgencyKPIBreakdowns([]agencyKPIBreakdown{
		{"weekday": {"morning": weekday}},
		{"weekend": {"morning": weekend}},
	})
	if *got["weekday"]["morning"].ServiceOperated.OperatedPct != 80 {
		t.Fatalf("weekday = %+v", got["weekday"]["morning"])
	}
	if *got["weekend"]["morning"].OnTimePerformance.OfScheduledPct != 33.3 {
		t.Fatalf("weekend = %+v", got["weekend"]["morning"])
	}
}
