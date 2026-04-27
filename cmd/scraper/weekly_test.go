package main

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

func TestDefaultWeekEndSaturday(t *testing.T) {
	pt, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("load PT: %v", err)
	}

	cases := []struct {
		name        string
		nowPT       string
		wantWeekEnd string
	}{
		// Sun → prior Sat (this is the cron's nominal fire time).
		{"sunday_morning_3am", "2026-04-26 03:00:00", "2026-04-25"},
		{"sunday_late",        "2026-04-26 23:59:59", "2026-04-25"},
		// Mid-week → still last Sat.
		{"monday",   "2026-04-27 12:00:00", "2026-04-25"},
		{"thursday", "2026-04-30 09:00:00", "2026-04-25"},
		{"friday",   "2026-05-01 18:00:00", "2026-04-25"},
		// Saturday today is not yet complete — go to last Saturday, not today.
		{"saturday_morning", "2026-05-02 06:00:00", "2026-04-25"},
		{"saturday_late",    "2026-05-02 23:59:59", "2026-04-25"},
		// Next Sunday rolls forward to the just-completed Sat.
		{"next_sunday", "2026-05-03 03:00:00", "2026-05-02"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now, err := time.ParseInLocation("2006-01-02 15:04:05", tc.nowPT, pt)
			if err != nil {
				t.Fatalf("parse now: %v", err)
			}
			got := defaultWeekEndSaturday(now)
			if got.String() != tc.wantWeekEnd {
				t.Fatalf("got %s, want %s (now PT = %s, weekday = %s)",
					got, tc.wantWeekEnd, tc.nowPT, civilWeekday(civil.DateOf(now.In(pt))))
			}
			if civilWeekday(got) != time.Saturday {
				t.Fatalf("returned date %s is not a Saturday (weekday=%s)", got, civilWeekday(got))
			}
		})
	}
}

// Sanity-check that civilWeekday returns the right weekday for known
// dates. Important because the rest of the file's date math hangs off
// it (week-rollback, day labeling).
func TestCivilWeekday(t *testing.T) {
	cases := map[string]time.Weekday{
		"2026-04-19": time.Sunday,
		"2026-04-20": time.Monday,
		"2026-04-25": time.Saturday,
		"2026-04-26": time.Sunday,
		"2026-01-01": time.Thursday,
	}
	for s, want := range cases {
		d, err := civil.ParseDate(s)
		if err != nil {
			t.Fatalf("parse %s: %v", s, err)
		}
		if got := civilWeekday(d); got != want {
			t.Fatalf("civilWeekday(%s) = %s, want %s", s, got, want)
		}
	}
}

// Sun→Sat indexing: dayNames[i] must match the weekday of weekStart+i,
// where weekStart is a Sunday. If this drifts, every chart's day label
// will be off-by-one.
func TestDayNamesAlignWithWeekStart(t *testing.T) {
	weekStart, _ := civil.ParseDate("2026-04-19") // Sunday
	if civilWeekday(weekStart) != time.Sunday {
		t.Fatalf("test fixture week start must be Sunday; got %s", civilWeekday(weekStart))
	}
	wantDays := []time.Weekday{
		time.Sunday, time.Monday, time.Tuesday, time.Wednesday,
		time.Thursday, time.Friday, time.Saturday,
	}
	for i, want := range wantDays {
		d := weekStart.AddDays(i)
		if got := civilWeekday(d); got != want {
			t.Fatalf("week start + %d = %s (weekday %s), want %s", i, d, got, want)
		}
		// dayNames[i] should match the abbreviated name of `want`.
		short := want.String()[:3]
		if dayNames[i] != short {
			t.Fatalf("dayNames[%d] = %q, want %q", i, dayNames[i], short)
		}
	}
}
