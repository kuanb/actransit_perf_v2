package main

import (
	"archive/zip"
	"bytes"
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func TestDefaultStatsServiceDate(t *testing.T) {
	pt, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("load PT: %v", err)
	}
	cases := []struct {
		name     string
		now      time.Time
		wantYear int
		wantMon  int
		wantDay  int
	}{
		{
			name:     "10pm PT → today's service date",
			now:      time.Date(2026, 4, 24, 22, 0, 0, 0, pt),
			wantYear: 2026, wantMon: 4, wantDay: 24,
		},
		{
			name:     "1am PT → yesterday's service date (before 4am rollover)",
			now:      time.Date(2026, 4, 25, 1, 30, 0, 0, pt),
			wantYear: 2026, wantMon: 4, wantDay: 24,
		},
		{
			name:     "4am PT exactly → today",
			now:      time.Date(2026, 4, 25, 4, 0, 0, 0, pt),
			wantYear: 2026, wantMon: 4, wantDay: 25,
		},
		{
			name:     "noon UTC mapped to PT today (4am+)",
			now:      time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC),
			wantYear: 2026, wantMon: 4, wantDay: 25,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := defaultStatsServiceDate(c.now)
			want := civil.Date{Year: c.wantYear, Month: time.Month(c.wantMon), Day: c.wantDay}
			if got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}

// buildSyntheticGTFSCalendarZip produces a zip with calendar.txt,
// calendar_dates.txt, trips.txt, stop_times.txt, routes.txt — the files
// loadActiveServices / loadScheduledTripRoutes / loadScheduledRuns /
// loadRouteColors read.
func buildSyntheticGTFSCalendarZip(t *testing.T) []byte {
	t.Helper()
	files := map[string]string{
		"calendar.txt": `service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
WKDY,1,1,1,1,1,0,0,20260101,20271231
SAT,0,0,0,0,0,1,0,20260101,20271231
`,
		"calendar_dates.txt": `service_id,date,exception_type
WKDY,20260427,2
HOLIDAY,20260427,1
`,
		"trips.txt": `trip_id,route_id,service_id,direction_id
T1,R1,WKDY,0
T2,R1,WKDY,0
T3,R2,WKDY,1
T4,R3,SAT,0
T5,R4,HOLIDAY,1
`,
		"stop_times.txt": `trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,timepoint
T1,08:00:00,08:00:00,S1,1,0,1
T1,08:10:00,08:10:00,S2,2,0,1
T1,08:20:00,08:20:00,S3,3,1,1
T2,09:00:00,09:00:00,S1,1,0,1
T2,09:10:00,09:10:00,S2,2,1,1
T2,09:20:00,09:20:00,S3,3,0,1
T2,09:30:00,09:30:00,S4,4,0,1
T3,10:00:00,10:00:00,S2,1,0,1
T3,10:10:00,10:10:00,S3,2,1,1
T4,11:00:00,11:00:00,S3,1,0,1
T5,12:00:00,12:00:00,S4,1,0,1
`,
		"routes.txt": `route_id,route_color,route_text_color
R1,A30D11,FFFFFF
R2,2B589C,FFFFFF
R3,,
R4,07B5D0,000000
`,
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func openZipReader(t *testing.T, data []byte) *zip.Reader {
	t.Helper()
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("zip reader: %v", err)
	}
	return zr
}

func TestLoadActiveServices(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))

	t.Run("monday picks up WKDY", func(t *testing.T) {
		// 2026-04-27 is a Monday
		got, err := loadActiveServices(zr, civil.Date{Year: 2026, Month: 4, Day: 27})
		if err != nil {
			t.Fatalf("loadActiveServices: %v", err)
		}
		// calendar_dates removes WKDY for that date and adds HOLIDAY
		if _, ok := got["WKDY"]; ok {
			t.Fatalf("WKDY should have been removed by calendar_dates exception")
		}
		if _, ok := got["HOLIDAY"]; !ok {
			t.Fatalf("HOLIDAY should be active via calendar_dates type=1")
		}
	})

	t.Run("saturday picks up SAT", func(t *testing.T) {
		// 2026-04-25 is a Saturday
		got, err := loadActiveServices(zr, civil.Date{Year: 2026, Month: 4, Day: 25})
		if err != nil {
			t.Fatalf("loadActiveServices: %v", err)
		}
		if _, ok := got["SAT"]; !ok {
			t.Fatalf("SAT not active on Saturday: %+v", got)
		}
		if _, ok := got["WKDY"]; ok {
			t.Fatalf("WKDY shouldn't be active on Saturday")
		}
	})

	t.Run("regular weekday picks up WKDY", func(t *testing.T) {
		// 2026-04-28 is a Tuesday — no calendar_dates exception
		got, err := loadActiveServices(zr, civil.Date{Year: 2026, Month: 4, Day: 28})
		if err != nil {
			t.Fatalf("loadActiveServices: %v", err)
		}
		if _, ok := got["WKDY"]; !ok {
			t.Fatalf("WKDY should be active on Tuesday")
		}
	})
}

func TestGTFSSupportsServiceDate(t *testing.T) {
	body := buildSyntheticGTFSCalendarZip(t)

	supported, err := gtfsSupportsServiceDate(body, civil.Date{Year: 2026, Month: 4, Day: 28})
	if err != nil {
		t.Fatalf("gtfsSupportsServiceDate: %v", err)
	}
	if !supported {
		t.Fatal("expected synthetic feed to support 2026-04-28")
	}

	supported, err = gtfsSupportsServiceDate(body, civil.Date{Year: 2030, Month: 1, Day: 1})
	if err != nil {
		t.Fatalf("gtfsSupportsServiceDate: %v", err)
	}
	if supported {
		t.Fatal("synthetic feed should not support 2030-01-01")
	}

	if _, err := gtfsSupportsServiceDate([]byte("not a zip"), civil.Date{Year: 2026, Month: 4, Day: 28}); err == nil {
		t.Fatal("invalid zip should return an error")
	}
}

func TestLoadScheduledTripRoutes(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))
	services := map[string]struct{}{"WKDY": {}}
	got, err := loadScheduledTripRoutes(zr, services)
	if err != nil {
		t.Fatalf("loadScheduledTripRoutes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d trips, want 3 (T1, T2, T3): %+v", len(got), got)
	}
	if got["T1"] != "R1" || got["T3"] != "R2" {
		t.Fatalf("trip→route mapping wrong: %+v", got)
	}
	if _, ok := got["T4"]; ok {
		t.Fatalf("T4 (SAT service) should not be in WKDY scheduled set")
	}
}

func TestLoadScheduledRuns(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))
	services := map[string]struct{}{"WKDY": {}}
	serviceDate := civil.Date{Year: 2026, Month: 4, Day: 28}
	got, err := loadScheduledRuns(zr, serviceDate, services)
	if err != nil {
		t.Fatalf("loadScheduledRuns: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d runs, want 3: %+v", len(got), got)
	}
	byTrip := make(map[string]scheduledRun, len(got))
	for _, run := range got {
		byTrip[run.TripID] = run
	}
	if got := byTrip["T1"]; got.RouteID != "R1" || got.DirectionID != "0" || got.Start.Hour() != 15 {
		t.Fatalf("T1 = %+v, want route R1 direction 0 at 08:00 PT", got)
	}
}

func TestLoadScheduledStopPlan(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))
	tripRoutes := map[string]string{"T1": "R1", "T2": "R1", "T3": "R2"}
	plan, err := loadScheduledStopPlan(zr, tripRoutes)
	if err != nil {
		t.Fatalf("loadScheduledStopPlan: %v", err)
	}

	if got := plan.ScoredByRoute["R1"]; got != 2 {
		t.Fatalf("R1 scored stops = %d, want 2", got)
	}
	if got := plan.ScoredByRoute["R2"]; got != 0 {
		t.Fatalf("R2 scored stops = %d, want 0", got)
	}
	for _, key := range []scheduledStopKey{{"T1", 2}, {"T2", 3}} {
		if _, ok := plan.ScoredStops[key]; !ok {
			t.Errorf("expected scored stop %+v", key)
		}
	}
	for _, key := range []scheduledStopKey{{"T1", 1}, {"T1", 3}, {"T2", 2}, {"T2", 4}} {
		if _, ok := plan.ScoredStops[key]; ok {
			t.Errorf("stop %+v should be excluded", key)
		}
	}
	if got := plan.LastBoardingSequence["T1"]; got != 2 {
		t.Fatalf("T1 last boarding sequence = %d, want 2", got)
	}
	if got := plan.LastBoardingSequence["T2"]; got != 3 {
		t.Fatalf("T2 last boarding sequence = %d, want 3", got)
	}
	if got := plan.LastBoardingSequence["T3"]; got != 1 {
		t.Fatalf("T3 last boarding sequence = %d, want 1", got)
	}
}

func TestComputeRouteStopSD(t *testing.T) {
	plan := &scheduledStopPlan{
		ScoredStops: map[scheduledStopKey]string{
			{"T1", 2}: "R1",
			{"T2", 3}: "R1",
			{"T3", 2}: "R2",
		},
		ScoredByRoute: map[string]int64{"R1": 2, "R2": 1},
	}
	delivered := []scheduledStopKey{{"T1", 2}, {"T1", 3}, {"OTHER", 2}}
	got := computeRouteStopSD(plan, delivered)
	if got["R1"].DeliveredN != 1 || got["R1"].TotalN != 2 || got["R1"].Pct != 50 {
		t.Fatalf("R1 = %+v, want 1/2 (50%%)", got["R1"])
	}
	if got["R2"].DeliveredN != 0 || got["R2"].TotalN != 1 || got["R2"].Pct != 0 {
		t.Fatalf("R2 = %+v, want 0/1 (0%%)", got["R2"])
	}
}

func TestComputeTripsNotCompleted(t *testing.T) {
	plan := &scheduledStopPlan{
		StopPositions: map[scheduledStopKey]int{
			{"T1", 1}: 1, {"T1", 2}: 2,
			{"T2", 1}: 1, {"T2", 2}: 2, {"T2", 3}: 3,
			{"T3", 1}: 1,
		},
		LastBoardingSequence: map[string]int64{"T1": 2, "T2": 3, "T3": 1},
		LastBoardingPosition: map[string]int{"T1": 2, "T2": 3, "T3": 1},
	}
	progress := map[string]int64{"T1": 1, "T2": 2, "T3": 1, "UNSCHEDULED": 7}

	count, dist := computeTripsNotCompleted(progress, plan)
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if dist == nil || dist.Histogram[5] != 1 || dist.Histogram[6] != 1 {
		t.Fatalf("distribution = %+v, want one trip each in 50%% and 60%% buckets", dist)
	}
}

func TestCountTwoBusGapWindows(t *testing.T) {
	base := time.Date(2026, 4, 28, 8, 0, 0, 0, time.UTC)
	runs := []scheduledRun{
		{TripID: "A1", RouteID: "A", DirectionID: "0", Start: base},
		{TripID: "A2", RouteID: "A", DirectionID: "0", Start: base.Add(time.Hour)},
		{TripID: "A3", RouteID: "A", DirectionID: "0", Start: base.Add(2 * time.Hour)},
		{TripID: "A4", RouteID: "A", DirectionID: "0", Start: base.Add(3 * time.Hour)},
		{TripID: "A5", RouteID: "A", DirectionID: "1", Start: base.Add(30 * time.Minute)},
		{TripID: "A6", RouteID: "A", DirectionID: "1", Start: base.Add(90 * time.Minute)},
		{TripID: "B1", RouteID: "B", DirectionID: "0", Start: base},
		{TripID: "B2", RouteID: "B", DirectionID: "0", Start: base.Add(time.Hour)},
	}
	observed := map[string]struct{}{"A4": {}, "A5": {}, "B1": {}}

	got := countTwoBusGapWindows(runs, observed)
	if got["A"] != 2 {
		t.Fatalf("route A windows = %d, want 2 from the A1/A2/A3 streak", got["A"])
	}
	if got["B"] != 0 {
		t.Fatalf("route B windows = %d, want 0 because B1 was observed", got["B"])
	}
}

func TestLoadRouteColors(t *testing.T) {
	zr := openZipReader(t, buildSyntheticGTFSCalendarZip(t))
	got, err := loadRouteColors(zr)
	if err != nil {
		t.Fatalf("loadRouteColors: %v", err)
	}
	if got["R1"].color != "A30D11" || got["R1"].text != "FFFFFF" {
		t.Fatalf("R1 colors = %+v, want A30D11/FFFFFF", got["R1"])
	}
	// Empty route_color/route_text_color should fall back to white/black.
	if got["R3"].color != "FFFFFF" || got["R3"].text != "000000" {
		t.Fatalf("R3 should default to FFFFFF/000000, got %+v", got["R3"])
	}
}

func TestComputeDistortion(t *testing.T) {
	t0 := time.Date(2026, 4, 24, 15, 0, 0, 0, time.UTC) // 8:00 AM PT, say
	// Schedule for one (route, stop): three buses at 10-min headway
	sched := map[stopKey][]time.Time{
		{"R1", "A"}: {
			t0,
			t0.Add(10 * time.Minute),
			t0.Add(20 * time.Minute),
		},
	}

	t.Run("late bus on first scheduled has no prior — skipped", func(t *testing.T) {
		obs := []observationRow{
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0, DelaySeconds: 60},
		}
		_, byRoute := computeDistortion(obs, sched)
		if len(byRoute["R1"]) != 0 {
			t.Fatalf("expected 0 distortions (no prior), got %v", byRoute["R1"])
		}
	})

	t.Run("late bus uses prior headway: +60% on 10-min headway", func(t *testing.T) {
		// scheduled = t0 + 10 min, delayed by 6 min → 360 / 600 = 60%
		obs := []observationRow{
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0.Add(10 * time.Minute), DelaySeconds: 360},
		}
		_, byRoute := computeDistortion(obs, sched)
		if len(byRoute["R1"]) != 1 {
			t.Fatalf("got %d, want 1", len(byRoute["R1"]))
		}
		got := byRoute["R1"][0]
		if got != 60 {
			t.Fatalf("distortion = %v, want 60", got)
		}
	})

	t.Run("early bus uses next headway: -100% when arriving at prior bus's slot", func(t *testing.T) {
		// scheduled = t0 + 10, delay = -600s (10 min early) → bus arrives at t0
		// Distortion based on next headway (10 min) = -600/600 = -100%
		obs := []observationRow{
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0.Add(10 * time.Minute), DelaySeconds: -600},
		}
		_, byRoute := computeDistortion(obs, sched)
		if len(byRoute["R1"]) != 1 || byRoute["R1"][0] != -100 {
			t.Fatalf("got %v, want -100", byRoute["R1"])
		}
	})

	t.Run("early bus on last scheduled has no next — skipped", func(t *testing.T) {
		obs := []observationRow{
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0.Add(20 * time.Minute), DelaySeconds: -60},
		}
		_, byRoute := computeDistortion(obs, sched)
		if len(byRoute["R1"]) != 0 {
			t.Fatalf("expected 0 distortions, got %v", byRoute["R1"])
		}
	})

	t.Run("histogram buckets (5%-wide, ±100% range, two extreme buckets)", func(t *testing.T) {
		obs := []observationRow{
			// Late 6 min on 10-min headway → +60% → bucket index for +60% to +65%
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0.Add(10 * time.Minute), DelaySeconds: 360},
			// Late 0.6 min on 10-min headway → +6% → bucket for +5% to +10%
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0.Add(20 * time.Minute), DelaySeconds: 36},
			// Late 12 min on 10-min headway → +120% → overflow bucket (last)
			{RouteID: "R1", StopID: "A", ScheduledArrival: t0.Add(10 * time.Minute), DelaySeconds: 720},
		}
		hist, _ := computeDistortion(obs, sched)
		if len(hist.Counts) != 42 {
			t.Fatalf("got %d buckets, want 42", len(hist.Counts))
		}
		// +60% → 1 + (60+100)/5 = 1 + 32 = 33
		if hist.Counts[33] != 1 {
			t.Fatalf("bucket[33] (+60..+65) count = %d, want 1", hist.Counts[33])
		}
		// +6% → 1 + (6+100)/5 = 1 + 21 = 22
		if hist.Counts[22] != 1 {
			t.Fatalf("bucket[22] (+5..+10) count = %d, want 1", hist.Counts[22])
		}
		// +120% (overflow) → bucket 41
		if hist.Counts[41] != 1 {
			t.Fatalf("bucket[41] (≥ +100%%) count = %d, want 1", hist.Counts[41])
		}
	})

	t.Run("distortionBucketIndex boundary cases", func(t *testing.T) {
		cases := []struct {
			d    float64
			want int
		}{
			{-200, 0},   // underflow
			{-100, 0},   // exactly -100% goes to underflow
			{-99.9, 1},  // first 5% bucket
			{-95, 2},    // -95..-90
			{0, 21},     // 0% → bucket starting at 0
			{4.99, 21},  // upper edge of 0..5
			{5, 22},     // start of +5..+10
			{99.99, 40}, // last 5% bucket
			{100, 41},   // exactly +100% goes to overflow
			{500, 41},   // overflow
		}
		for _, c := range cases {
			if got := distortionBucketIndex(c.d); got != c.want {
				t.Fatalf("distortionBucketIndex(%v) = %d, want %d", c.d, got, c.want)
			}
		}
	})

	t.Run("unknown stop is skipped without panicking", func(t *testing.T) {
		obs := []observationRow{
			{RouteID: "R1", StopID: "UNKNOWN", ScheduledArrival: t0, DelaySeconds: 60},
		}
		_, byRoute := computeDistortion(obs, sched)
		if len(byRoute["R1"]) != 0 {
			t.Fatalf("expected 0, got %v", byRoute["R1"])
		}
	})
}

func TestPercentileSorted(t *testing.T) {
	cases := []struct {
		name string
		vals []float64
		p    float64
		want float64
	}{
		{"empty", nil, 0.5, 0},
		{"p50 of 10 values", []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0.5, 5},
		{"p95 of 100 values", func() []float64 {
			s := make([]float64, 100)
			for i := range s {
				s[i] = float64(i)
			}
			return s
		}(), 0.95, 95},
		{"p100 clamps", []float64{1, 2, 3}, 1.0, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := percentileSorted(c.vals, c.p); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestBuildVolumeHistogram(t *testing.T) {
	mk := func(at int, n int64) []int64 {
		c := make([]int64, 96)
		c[at] = n
		return c
	}
	byRoute := map[string][]int64{
		"99": mk(10, 1000),
		"1":  mk(10, 100),
		"6":  mk(10, 80),
		"18": mk(10, 60),
		"51": mk(10, 50),
		"57": mk(10, 40),
		"72": mk(10, 30),
		"NL": mk(10, 25),
		"O":  mk(10, 20),
		"F":  mk(10, 15),
		"G":  mk(10, 12),
		"H":  mk(10, 8),
		"J":  mk(10, 7),
	}
	scheduledByRoute := map[string]int{
		"99": 2,
		"1":  20, "6": 20, "18": 20, "51": 20, "57": 20, "72": 20,
		"NL": 5, "O": 5, "F": 5, "G": 5, "H": 5, "J": 5,
	}
	colors := map[string]colorPair{
		"1":  {color: "AA0000", text: "FFFFFF"},
		"6":  {color: "BB0000", text: "FFFFFF"},
		"18": {color: "CC0000", text: "FFFFFF"},
		"51": {color: "DD0000", text: "FFFFFF"},
		"57": {color: "EE0000", text: "FFFFFF"},
		"72": {color: "FF0000", text: "FFFFFF"},
		"NL": {color: "00AA00", text: "FFFFFF"},
		"O":  {color: "00BB00", text: "FFFFFF"},
		"F":  {color: "00CC00", text: "FFFFFF"},
		"G":  {color: "00DD00", text: "FFFFFF"},
		// "H" and "J" intentionally absent — should still roll into "Other".
	}
	got := buildVolumeHistogram(byRoute, colors, scheduledByRoute, 10)

	if len(got.Routes) != 11 {
		t.Fatalf("len(Routes) = %d, want 11 (top 10 + Other)", len(got.Routes))
	}
	if got.Routes[0].RouteID != "1" || got.Routes[0].Color != "AA0000" {
		t.Fatalf("top route = %+v, want id=1 color=AA0000", got.Routes[0])
	}
	last := got.Routes[len(got.Routes)-1]
	if last.RouteID != "Other" {
		t.Fatalf("last series = %s, want Other", last.RouteID)
	}
	if last.Counts[10] != 8+7 {
		t.Fatalf("Other counts[10] = %d, want %d", last.Counts[10], 8+7)
	}

	var systemAt10 int64
	for rid, r := range byRoute {
		if rid == "99" {
			continue
		}
		systemAt10 += r[10]
	}
	if got.Totals[10] != systemAt10 {
		t.Fatalf("Totals[10] = %d, want %d (sum across included routes)", got.Totals[10], systemAt10)
	}
	if len(got.Totals) != 96 {
		t.Fatalf("len(Totals) = %d, want 96", len(got.Totals))
	}
}

func TestBuildVolumeHistogramNoOtherWhenWithinTopN(t *testing.T) {
	byRoute := map[string][]int64{
		"1": {1: 5},
		"2": {1: 3},
	}
	byRoute["1"] = append(byRoute["1"], make([]int64, 94)...)
	byRoute["2"] = append(byRoute["2"], make([]int64, 94)...)
	got := buildVolumeHistogram(byRoute, map[string]colorPair{}, map[string]int{"1": 20, "2": 20}, 10)
	if len(got.Routes) != 2 {
		t.Fatalf("len(Routes) = %d, want 2 (no Other when all fit in topN)", len(got.Routes))
	}
	for _, r := range got.Routes {
		if r.RouteID == "Other" {
			t.Fatalf("unexpected Other series when nothing collapses")
		}
	}
}

func TestIsLimitedRoute(t *testing.T) {
	cases := []struct {
		routeID     string
		scheduled   float64
		wantLimited bool
	}{
		{routeID: "99", scheduled: 9, wantLimited: true},
		{routeID: "99", scheduled: 10, wantLimited: false},
		{routeID: "1T", scheduled: 9, wantLimited: true},
		{routeID: "O", scheduled: 2, wantLimited: false},
		{routeID: "NL", scheduled: 2, wantLimited: false},
	}
	for _, tc := range cases {
		if got := isLimitedRoute(tc.routeID, tc.scheduled); got != tc.wantLimited {
			t.Errorf("isLimitedRoute(%q, %v) = %v, want %v", tc.routeID, tc.scheduled, got, tc.wantLimited)
		}
	}
}

func TestRound1(t *testing.T) {
	cases := []struct {
		in   float64
		want float64
	}{
		{0, 0},
		{1.23, 1.2},
		{1.25, 1.3},
		{-1.25, -1.3},
		{99.95, 100.0},
	}
	for _, c := range cases {
		if got := round1(c.in); got != c.want {
			t.Fatalf("round1(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
