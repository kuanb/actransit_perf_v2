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
// calendar_dates.txt, trips.txt, routes.txt — the files loadActiveServices /
// loadScheduledTripRoutes / loadRouteColors read.
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
		"trips.txt": `trip_id,route_id,service_id
T1,R1,WKDY
T2,R1,WKDY
T3,R2,WKDY
T4,R3,SAT
T5,R4,HOLIDAY
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
