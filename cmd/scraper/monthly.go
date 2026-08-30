package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/civil"
)

const (
	monthlyArchivePrefix = "stats/monthly/"
	monthlyLatestKey     = "stats/monthly/latest.json"
	monthlyIndexKey      = "stats/monthly/_index.json"
)

type monthlyStats struct {
	MethodologyVersion int               `json:"methodology_version"`
	Month              string            `json:"month"`
	MonthStart         string            `json:"month_start"`
	MonthEnd           string            `json:"month_end"`
	GeneratedAt        time.Time         `json:"generated_at"`
	Status             string            `json:"status"`
	DaysExpected       int               `json:"days_expected"`
	DaysAvailable      int               `json:"days_available"`
	AgencyKPI          agencyKPIStats    `json:"agency_kpi"`
	Weeks              []monthlyKPIWeek  `json:"weeks"`
	Routes             []monthlyRouteKPI `json:"routes"`
}

type monthlyKPIWeek struct {
	WeekStart     string               `json:"week_start"`
	PeriodStart   string               `json:"period_start"`
	PeriodEnd     string               `json:"period_end"`
	Status        string               `json:"status"`
	DaysExpected  int                  `json:"days_expected"`
	DaysAvailable int                  `json:"days_available"`
	AgencyKPI     agencyKPIStats       `json:"agency_kpi"`
	DailyRange    monthlyKPIDailyRange `json:"daily_range"`
}

type monthlyKPIDailyRange struct {
	ServiceOperated percentageRange `json:"service_operated"`
	OTPOfOperated   percentageRange `json:"otp_of_operated"`
	OTPOfScheduled  percentageRange `json:"otp_of_scheduled"`
}

type percentageRange struct {
	MinPct *float64 `json:"min_pct,omitempty"`
	MaxPct *float64 `json:"max_pct,omitempty"`
}

type monthlyRouteKPI struct {
	RouteID   string         `json:"route_id"`
	Color     string         `json:"color"`
	TextColor string         `json:"text_color"`
	AgencyKPI agencyKPIStats `json:"agency_kpi"`
}

func generateMonthlyStats(ctx context.Context, monthStart civil.Date) (*monthlyStats, error) {
	if monthStart.Day != 1 {
		return nil, fmt.Errorf("month must start on day 1; got %s", monthStart)
	}
	monthEnd := lastDayOfMonth(monthStart)
	dailies, err := readDailyStatsForRange(ctx, monthStart, monthEnd)
	if err != nil {
		return nil, err
	}
	out := aggregateMonthlyStats(monthStart, dailies, time.Now().UTC())
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	key := fmt.Sprintf("%s%s.json", monthlyArchivePrefix, out.Month)
	if err := writeObject(ctx, key, payload); err != nil {
		return nil, fmt.Errorf("write %s: %w", key, err)
	}
	updateLatest, err := isMonthlyAtLeastAsRecentAsLatest(ctx, monthStart)
	if err != nil {
		return nil, fmt.Errorf("compare monthly latest: %w", err)
	}
	if updateLatest {
		if err := writeObject(ctx, monthlyLatestKey, payload); err != nil {
			return nil, fmt.Errorf("write monthly latest: %w", err)
		}
	}
	if err := updateMonthlyIndex(ctx, out.Month); err != nil {
		return out, fmt.Errorf("update monthly index: %w", err)
	}
	return out, nil
}

func readDailyStatsForRange(ctx context.Context, start, end civil.Date) ([]*dailyStats, error) {
	count := daysInclusive(start, end)
	out := make([]*dailyStats, count)
	for i := 0; i < count; i++ {
		date := start.AddDays(i)
		key := fmt.Sprintf("%s%s.json", statsArchivePrefix, date)
		body, exists, err := readObject(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", key, err)
		}
		if !exists {
			continue
		}
		var daily dailyStats
		if err := json.Unmarshal(body, &daily); err != nil {
			return nil, fmt.Errorf("parse %s: %w", key, err)
		}
		out[i] = &daily
	}
	return out, nil
}

func aggregateMonthlyStats(monthStart civil.Date, dailies []*dailyStats, generatedAt time.Time) *monthlyStats {
	monthEnd := lastDayOfMonth(monthStart)
	out := &monthlyStats{
		MethodologyVersion: agencyKPIMethodologyVersion,
		Month:              fmt.Sprintf("%04d-%02d", monthStart.Year, monthStart.Month),
		MonthStart:         monthStart.String(),
		MonthEnd:           monthEnd.String(),
		GeneratedAt:        generatedAt,
		DaysExpected:       daysInclusive(monthStart, monthEnd),
	}

	type weekAccum struct {
		start  civil.Date
		end    civil.Date
		values []agencyKPIStats
	}
	weekOrder := make([]string, 0, 6)
	weeks := make(map[string]*weekAccum)
	type routeAccum struct {
		routeID   string
		color     string
		textColor string
		values    []agencyKPIStats
	}
	routes := make(map[string]*routeAccum)
	var systemValues []agencyKPIStats

	for i := 0; i < out.DaysExpected; i++ {
		date := monthStart.AddDays(i)
		weekStart := date.AddDays(-int(civilWeekday(date)))
		key := weekStart.String()
		week := weeks[key]
		if week == nil {
			periodStart := weekStart
			if periodStart.Before(monthStart) {
				periodStart = monthStart
			}
			periodEnd := weekStart.AddDays(6)
			if monthEnd.Before(periodEnd) {
				periodEnd = monthEnd
			}
			week = &weekAccum{start: periodStart, end: periodEnd}
			weeks[key] = week
			weekOrder = append(weekOrder, key)
		}
		if i >= len(dailies) || dailies[i] == nil || dailies[i].AgencyKPI.MethodologyVersion != agencyKPIMethodologyVersion {
			continue
		}
		daily := dailies[i]
		out.DaysAvailable++
		systemValues = append(systemValues, daily.AgencyKPI)
		week.values = append(week.values, daily.AgencyKPI)
		for _, route := range daily.Routes {
			if route.AgencyKPI.MethodologyVersion != agencyKPIMethodologyVersion {
				continue
			}
			acc := routes[route.RouteID]
			if acc == nil {
				acc = &routeAccum{routeID: route.RouteID}
				routes[route.RouteID] = acc
			}
			if route.Color != "" {
				acc.color = route.Color
			}
			if route.TextColor != "" {
				acc.textColor = route.TextColor
			}
			acc.values = append(acc.values, route.AgencyKPI)
		}
	}

	out.Status = completenessStatus(out.DaysAvailable, out.DaysExpected)
	out.AgencyKPI = aggregateAgencyKPIStats(systemValues)
	for _, key := range weekOrder {
		week := weeks[key]
		expected := daysInclusive(week.start, week.end)
		out.Weeks = append(out.Weeks, monthlyKPIWeek{
			WeekStart:     key,
			PeriodStart:   week.start.String(),
			PeriodEnd:     week.end.String(),
			Status:        completenessStatus(len(week.values), expected),
			DaysExpected:  expected,
			DaysAvailable: len(week.values),
			AgencyKPI:     aggregateAgencyKPIStats(week.values),
			DailyRange:    dailyKPIPercentageRange(week.values),
		})
	}
	for _, route := range routes {
		out.Routes = append(out.Routes, monthlyRouteKPI{
			RouteID:   route.routeID,
			Color:     route.color,
			TextColor: route.textColor,
			AgencyKPI: aggregateAgencyKPIStats(route.values),
		})
	}
	sort.Slice(out.Routes, func(i, j int) bool {
		return out.Routes[i].RouteID < out.Routes[j].RouteID
	})
	return out
}

func dailyKPIPercentageRange(values []agencyKPIStats) monthlyKPIDailyRange {
	var serviceOperated, otpOfOperated, otpOfScheduled []float64
	for _, value := range values {
		if value.ServiceOperated.OperatedPct != nil {
			serviceOperated = append(serviceOperated, *value.ServiceOperated.OperatedPct)
		}
		if value.OnTimePerformance.OfOperatedPct != nil {
			otpOfOperated = append(otpOfOperated, *value.OnTimePerformance.OfOperatedPct)
		}
		if value.OnTimePerformance.OfScheduledPct != nil {
			otpOfScheduled = append(otpOfScheduled, *value.OnTimePerformance.OfScheduledPct)
		}
	}
	return monthlyKPIDailyRange{
		ServiceOperated: percentageMinMax(serviceOperated),
		OTPOfOperated:   percentageMinMax(otpOfOperated),
		OTPOfScheduled:  percentageMinMax(otpOfScheduled),
	}
}

func percentageMinMax(values []float64) percentageRange {
	if len(values) == 0 {
		return percentageRange{}
	}
	minPct, maxPct := values[0], values[0]
	for _, value := range values[1:] {
		minPct = min(minPct, value)
		maxPct = max(maxPct, value)
	}
	return percentageRange{MinPct: &minPct, MaxPct: &maxPct}
}

func defaultMonthlyStatsMonth(now time.Time) civil.Date {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	today := now.In(loc)
	lastMonth := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, loc).AddDate(0, 0, -1)
	return civil.Date{Year: lastMonth.Year(), Month: lastMonth.Month(), Day: 1}
}

func parseMonth(value string) (civil.Date, error) {
	t, err := time.Parse("2006-01", value)
	if err != nil {
		return civil.Date{}, err
	}
	return civil.Date{Year: t.Year(), Month: t.Month(), Day: 1}, nil
}

func lastDayOfMonth(monthStart civil.Date) civil.Date {
	last := time.Date(monthStart.Year, monthStart.Month+1, 0, 0, 0, 0, 0, time.UTC)
	return civil.DateOf(last)
}

func daysInclusive(start, end civil.Date) int {
	startTime := time.Date(start.Year, start.Month, start.Day, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(end.Year, end.Month, end.Day, 0, 0, 0, 0, time.UTC)
	return int(endTime.Sub(startTime)/(24*time.Hour)) + 1
}

func completenessStatus(available, expected int) string {
	switch {
	case available == 0:
		return "missing"
	case available == expected:
		return "complete"
	default:
		return "partial"
	}
}

func isMonthlyAtLeastAsRecentAsLatest(ctx context.Context, candidate civil.Date) (bool, error) {
	body, exists, err := readObject(ctx, monthlyLatestKey)
	if err != nil || !exists {
		return !exists, err
	}
	var latest monthlyStats
	if err := json.Unmarshal(body, &latest); err != nil {
		return false, err
	}
	existing, err := parseMonth(latest.Month)
	if err != nil {
		return false, err
	}
	return !candidate.Before(existing), nil
}

type monthlyIndex struct {
	Months    []string  `json:"months"`
	UpdatedAt time.Time `json:"updated_at"`
}

func updateMonthlyIndex(ctx context.Context, month string) error {
	idx := monthlyIndex{}
	body, exists, err := readObject(ctx, monthlyIndexKey)
	if err == nil && exists {
		_ = json.Unmarshal(body, &idx)
	}
	for _, existing := range idx.Months {
		if existing == month {
			idx.UpdatedAt = time.Now().UTC()
			payload, _ := json.MarshalIndent(idx, "", "  ")
			return writeObject(ctx, monthlyIndexKey, payload)
		}
	}
	idx.Months = append(idx.Months, month)
	sort.Sort(sort.Reverse(sort.StringSlice(idx.Months)))
	idx.UpdatedAt = time.Now().UTC()
	payload, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return writeObject(ctx, monthlyIndexKey, payload)
}
