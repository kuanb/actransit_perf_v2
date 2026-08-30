package main

import (
	"encoding/json"
	"testing"
)

func TestNormalizePublishedServiceOperated(t *testing.T) {
	var chart publishedChart
	if err := json.Unmarshal([]byte(`{
		"Id":"120-3",
		"Data":[
			{"REPORTFROMDATE":"2026-07-01T00:00:00","Trips Operated":89.93,"Target":99.5},
			{"REPORTFROMDATE":"2026-06-01T00:00:00","Trips Operated":94.24,"Target":99.5}
		]
	}`), &chart); err != nil {
		t.Fatal(err)
	}
	got, err := normalizePublishedServiceOperated(chart)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Month != "2026-06" || got[1].Pct != 89.93 || *got[0].TargetPct != 99.5 {
		t.Fatalf("normalized values = %+v", got)
	}
}

func TestNormalizePublishedOnTimeUnpivotsFiscalYears(t *testing.T) {
	var chart publishedChart
	if err := json.Unmarshal([]byte(`{
		"Id":"10",
		"Data":[
			{"Month":"2023-07-01T00:00:00","FY 24-25":76.92,"FY 25-26":77.87,"FY 26-27":null,"Target":75},
			{"Month":"2024-01-01T00:00:00","FY 24-25":75.69,"FY 25-26":72.19,"FY 26-27":null,"Target":75}
		]
	}`), &chart); err != nil {
		t.Fatal(err)
	}
	got, err := normalizePublishedOnTime(chart)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"2024-07": 76.92,
		"2025-01": 75.69,
		"2025-07": 77.87,
		"2026-01": 72.19,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d values, want %d: %+v", len(got), len(want), got)
	}
	for _, value := range got {
		if want[value.Month] != value.Pct || value.TargetPct == nil || *value.TargetPct != 75 {
			t.Fatalf("unexpected value %+v", value)
		}
	}
}

func TestNormalizePublishedServiceOperatedRejectsDuplicateMonth(t *testing.T) {
	var chart publishedChart
	if err := json.Unmarshal([]byte(`{
		"Id":"120-3",
		"Data":[
			{"REPORTFROMDATE":"2026-07-01T00:00:00","Trips Operated":90},
			{"REPORTFROMDATE":"2026-07-01T00:00:00","Trips Operated":91}
		]
	}`), &chart); err != nil {
		t.Fatal(err)
	}
	if _, err := normalizePublishedServiceOperated(chart); err == nil {
		t.Fatal("duplicate month should fail")
	}
}
