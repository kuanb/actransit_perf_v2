package main

import (
	"archive/zip"
	"bytes"
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func testSchoolCalendar(date civil.Date) []schoolCalendarPeriod {
	return []schoolCalendarPeriod{{
		Name:      "test year",
		StartDate: date,
		EndDate:   civil.Date{Year: date.Year + 1, Month: date.Month, Day: date.Day},
		SourceURL: "https://example.com/calendar",
	}}
}

func TestValidateSchoolDefinitionsRejectsOverlaps(t *testing.T) {
	definitions := []schoolDefinition{{
		ID:   "school",
		Name: "School",
		Routes: []schoolRouteTarget{
			{RouteID: "1", StopIDs: []string{"S"}},
		},
		StartPeriods: []schoolStartPeriod{
			{
				ValidFrom:  civil.Date{Year: 2026, Month: 1, Day: 1},
				ValidUntil: civil.Date{Year: 2026, Month: 9, Day: 1},
				StartTime:  "08:30",
			},
			{
				ValidFrom: civil.Date{Year: 2026, Month: 8, Day: 1},
				StartTime: "08:45",
			},
		},
		CalendarPeriods: testSchoolCalendar(civil.Date{Year: 2026, Month: 1, Day: 1}),
	}}
	if err := validateSchoolDefinitions(definitions); err == nil {
		t.Fatal("expected overlapping periods to fail validation")
	}
}

func TestSchoolStartPeriodForUsesHalfOpenRange(t *testing.T) {
	periods := []schoolStartPeriod{
		{
			ValidFrom:  civil.Date{Year: 2026, Month: 1, Day: 1},
			ValidUntil: civil.Date{Year: 2026, Month: 9, Day: 1},
			StartTime:  "08:30",
		},
		{
			ValidFrom: civil.Date{Year: 2026, Month: 9, Day: 1},
			StartTime: "08:45",
		},
	}
	period, ok := schoolStartPeriodFor(civil.Date{Year: 2026, Month: 8, Day: 31}, periods)
	if !ok || period.StartTime != "08:30" {
		t.Fatalf("August period = %+v, %v", period, ok)
	}
	period, ok = schoolStartPeriodFor(civil.Date{Year: 2026, Month: 9, Day: 1}, periods)
	if !ok || period.StartTime != "08:45" {
		t.Fatalf("September period = %+v, %v", period, ok)
	}
}

func TestSchoolCalendarPeriodFor(t *testing.T) {
	tests := []struct {
		name string
		date civil.Date
		want bool
	}{
		{name: "first day", date: civil.Date{Year: 2026, Month: 8, Day: 18}, want: true},
		{name: "weekend", date: civil.Date{Year: 2026, Month: 8, Day: 22}, want: false},
		{name: "district holiday", date: civil.Date{Year: 2026, Month: 9, Day: 7}, want: false},
		{name: "minimum day", date: civil.Date{Year: 2026, Month: 10, Day: 22}, want: true},
		{name: "winter recess", date: civil.Date{Year: 2026, Month: 12, Day: 21}, want: false},
		{name: "classes reconvene", date: civil.Date{Year: 2027, Month: 1, Day: 4}, want: true},
		{name: "secondary work day", date: civil.Date{Year: 2027, Month: 1, Day: 19}, want: false},
		{name: "last day", date: civil.Date{Year: 2027, Month: 6, Day: 10}, want: true},
		{name: "summer", date: civil.Date{Year: 2027, Month: 6, Day: 11}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, got := schoolCalendarPeriodFor(test.date, []schoolCalendarPeriod{wccusd2026SchoolYear})
			if got != test.want {
				t.Fatalf("schoolCalendarPeriodFor(%s) = %v, want %v", test.date, got, test.want)
			}
		})
	}
}

func TestDetectTripDepartures(t *testing.T) {
	cache := fixtureRouteCache()
	origin := cache.Routes["R1"].Stops["A"]
	t0 := time.Date(2026, 4, 24, 15, 0, 0, 0, time.UTC)

	t.Run("inside-to-outside crossing is confirmed", func(t *testing.T) {
		state := stateFile{InFlight: []inFlightTrip{{
			RouteID: "R1",
			TripID:  "T1",
			Probes: []probe{
				{TS: t0, Lat: origin.Lat, Lon: origin.Lon + 0.001},
				{TS: t0.Add(time.Minute), Lat: origin.Lat, Lon: origin.Lon + 0.003},
			},
		}}}
		var stats trackStats
		detectTripDepartures(&state, cache, &stats)
		trip := state.InFlight[0]
		if trip.DepartureConfidence != departureConfidenceConfirmed {
			t.Fatalf("confidence = %q", trip.DepartureConfidence)
		}
		if !trip.ActualDeparture.After(t0) || !trip.ActualDeparture.Before(t0.Add(time.Minute)) {
			t.Fatalf("departure = %v, want interpolated crossing", trip.ActualDeparture)
		}
	})

	t.Run("first probe outside is lower confidence", func(t *testing.T) {
		state := stateFile{InFlight: []inFlightTrip{{
			RouteID: "R1",
			TripID:  "T1",
			Probes:  []probe{{TS: t0, Lat: origin.Lat, Lon: origin.Lon + 0.003}},
		}}}
		detectTripDepartures(&state, cache, &trackStats{})
		trip := state.InFlight[0]
		if trip.DepartureConfidence != departureConfidenceFirstSeen {
			t.Fatalf("confidence = %q", trip.DepartureConfidence)
		}
		if !trip.ActualDeparture.Equal(t0) {
			t.Fatalf("departure = %v, want %v", trip.ActualDeparture, t0)
		}
	})
}

func TestDetectSchoolArrival(t *testing.T) {
	stop := gtfsStop{Lat: 0, Lon: 0}
	t0 := time.Date(2026, 4, 24, 15, 0, 0, 0, time.UTC)
	arrival, confidence, ok := detectSchoolArrival([]probe{
		{TS: t0, Lat: 0, Lon: 0.003},
		{TS: t0.Add(time.Minute), Lat: 0, Lon: 0.001},
	}, stop)
	if !ok {
		t.Fatal("arrival not detected")
	}
	if confidence != arrivalConfidenceConfirmed {
		t.Fatalf("confidence = %q", confidence)
	}
	if !arrival.After(t0) || !arrival.Before(t0.Add(time.Minute)) {
		t.Fatalf("arrival = %v, want interpolated crossing", arrival)
	}
}

func TestBuildSchoolStatsClassifiesArrivalAndDeparture(t *testing.T) {
	serviceDate := civil.Date{Year: 2026, Month: 4, Day: 24}
	start := schoolStartTimestamp(serviceDate, "08:30")
	scheduled := []scheduledSchoolTrip{{
		SchoolID:           "school",
		StartTime:          start,
		RouteID:            "1",
		TripID:             "T1",
		StopID:             "S",
		Stop:               gtfsStop{StopID: "S", Lat: 0, Lon: 0},
		ScheduledDeparture: start.Add(-time.Hour),
		ScheduledArrival:   start.Add(-15 * time.Minute),
	}}
	departure := start.Add(-40 * time.Minute)
	delay := 20.0
	observations := map[string]schoolObservationSummary{
		"T1": {
			VehicleID:             "V1",
			ActualDeparture:       &departure,
			DepartureConfidence:   departureConfidenceConfirmed,
			DepartureDelayMinutes: &delay,
			ObservedRunning:       true,
		},
	}
	probes := map[string][]probe{
		"T1": {
			{TS: start.Add(time.Minute), Lat: 0, Lon: 0.003},
			{TS: start.Add(3 * time.Minute), Lat: 0, Lon: 0.001},
		},
	}
	definitions := []schoolDefinition{{
		ID:              "school",
		Name:            "School",
		StartPeriods:    []schoolStartPeriod{{ValidFrom: serviceDate, StartTime: "08:30"}},
		CalendarPeriods: testSchoolCalendar(serviceDate),
		Routes:          []schoolRouteTarget{{RouteID: "1", StopIDs: []string{"S"}}},
	}}

	got := buildSchoolStats(serviceDate, scheduled, observations, probes, definitions)
	trip := got.Schools[0].Trips[0]
	if trip.DepartureStatus != "more_than_15_minutes_late" {
		t.Fatalf("departure status = %q", trip.DepartureStatus)
	}
	if trip.ArrivalStatus != "after_school_start" {
		t.Fatalf("arrival status = %q", trip.ArrivalStatus)
	}
	if got.Schools[0].ArrivalsAfterStart != 1 {
		t.Fatalf("late arrivals = %d", got.Schools[0].ArrivalsAfterStart)
	}
	if got.Schools[0].SchoolCalendarStart != serviceDate.String() || got.Schools[0].SchoolCalendarSource == "" {
		t.Fatalf("school calendar metadata = %+v", got.Schools[0])
	}
}

func TestLoadScheduledSchoolTrips(t *testing.T) {
	files := map[string]string{
		"trips.txt": "trip_id,route_id,service_id,direction_id\nT1,R1,WKDY,1\nT2,R1,WKDY,1\nT3,R2,WKDY,1\n",
		"stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n" +
			"T1,07:30:00,07:30:00,O,1\nT1,08:15:00,08:15:00,S,2\n" +
			"T2,13:30:00,13:30:00,O,1\nT2,14:15:00,14:15:00,S,2\n" +
			"T3,07:30:00,07:30:00,O,1\nT3,08:15:00,08:15:00,S,2\n",
		"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nO,Origin,0,0\nS,School,0,0.01\n",
	}
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, body := range files {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	date := civil.Date{Year: 2026, Month: 4, Day: 24}
	definitions := []schoolDefinition{{
		ID:              "school",
		Name:            "School",
		StartPeriods:    []schoolStartPeriod{{ValidFrom: date, StartTime: "08:30"}},
		CalendarPeriods: testSchoolCalendar(date),
		Routes:          []schoolRouteTarget{{RouteID: "R1", StopIDs: []string{"S"}}},
	}}
	got, err := loadScheduledSchoolTrips(zr, date, map[string]struct{}{"WKDY": {}}, definitions)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TripID != "T1" {
		t.Fatalf("scheduled trips = %+v, want T1", got)
	}
}
