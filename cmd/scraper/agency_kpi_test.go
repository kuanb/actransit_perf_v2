package main

import "testing"

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
	if len(plan.ByTrip["T1"]) != 1 || plan.ByTrip["T1"][0] != (scheduledStopKey{"T1", 2}) {
		t.Fatalf("T1 timepoints = %+v, want only interior stop 2", plan.ByTrip["T1"])
	}
	if len(plan.ByTrip["T2"]) != 1 || plan.ByTrip["T2"][0] != (scheduledStopKey{"T2", 3}) {
		t.Fatalf("T2 timepoints = %+v, want pickup-eligible interior stop 3", plan.ByTrip["T2"])
	}
	if len(plan.ByTrip["T3"]) != 0 {
		t.Fatalf("T3 timepoints = %+v, want none because it has no interior stop", plan.ByTrip["T3"])
	}
}

func TestBuildAgencyKPIStats(t *testing.T) {
	scheduled := map[string]string{"T1": "R1", "T2": "R1", "T3": "R2"}
	stopPlan := &scheduledStopPlan{
		LastBoardingSequence: map[string]int64{"T1": 2, "T2": 3, "T3": 2},
	}
	timepoints := &kpiTimepointPlan{ByTrip: map[string][]scheduledStopKey{
		"T1": {{"T1", 2}},
		"T2": {{"T2", 3}},
		"T3": {{"T3", 2}},
	}}
	observed := map[string]observedTripProgress{
		"T1":          {LastStopSequence: 2, HasStopSequence: true},
		"T2":          {LastStopSequence: 2, HasStopSequence: true},
		"UNSCHEDULED": {LastStopSequence: 10, HasStopSequence: true},
	}
	onTime := map[scheduledStopKey]struct{}{{"T1", 2}: {}, {"T3", 2}: {}}

	system, routes, err := buildAgencyKPIStats(
		scheduled,
		stopPlan,
		timepoints,
		observed,
		map[string]int64{},
		onTime,
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
	system, _, err := buildAgencyKPIStats(
		map[string]string{"T1": "R1"},
		&scheduledStopPlan{LastBoardingSequence: map[string]int64{"T1": 4}},
		&kpiTimepointPlan{ByTrip: map[string][]scheduledStopKey{}},
		map[string]observedTripProgress{"T1": {}},
		map[string]int64{"T1": 4},
		map[scheduledStopKey]struct{}{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if system.ServiceOperated.PartialTrips != 0 {
		t.Fatalf("partial trips = %d, want 0", system.ServiceOperated.PartialTrips)
	}
}
