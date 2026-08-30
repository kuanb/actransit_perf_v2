package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"cloud.google.com/go/civil"
)

type agencyKPIBackfillResult struct {
	StartDate      string `json:"start_date"`
	EndDate        string `json:"end_date"`
	DailyUpdated   int    `json:"daily_updated"`
	DailyMissing   int    `json:"daily_missing"`
	WeeklyUpdated  int    `json:"weekly_updated"`
	WeeklyMissing  int    `json:"weekly_missing"`
	MonthlyUpdated int    `json:"monthly_updated"`
}

func handleBackfillAgencyKPIs(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	startDate, err := civil.ParseDate(r.URL.Query().Get("start_date"))
	if err != nil {
		http.Error(w, "invalid start_date: "+err.Error(), http.StatusBadRequest)
		return
	}
	endDate, err := civil.ParseDate(r.URL.Query().Get("end_date"))
	if err != nil {
		http.Error(w, "invalid end_date: "+err.Error(), http.StatusBadRequest)
		return
	}
	if endDate.Before(startDate) || daysInclusive(startDate, endDate) > 40 {
		http.Error(w, "date range must be ordered and no longer than 40 days", http.StatusBadRequest)
		return
	}
	result, err := processAgencyKPIBackfill(r.Context(), startDate, endDate, r.URL.Query().Get("force") == "true")
	if err != nil {
		slog.Error("backfill-agency-kpis failed", "start_date", startDate, "end_date", endDate, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("backfill-agency-kpis ok",
		"duration_ms", time.Since(started).Milliseconds(),
		"start_date", startDate,
		"end_date", endDate,
		"daily_updated", result.DailyUpdated,
		"weekly_updated", result.WeeklyUpdated,
		"monthly_updated", result.MonthlyUpdated,
	)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func processAgencyKPIBackfill(ctx context.Context, start, end civil.Date, force bool) (*agencyKPIBackfillResult, error) {
	result := &agencyKPIBackfillResult{StartDate: start.String(), EndDate: end.String()}
	if end.Before(start) {
		return result, fmt.Errorf("end_date must not be before start_date")
	}
	if daysInclusive(start, end) > 40 {
		return result, fmt.Errorf("date range is limited to 40 days")
	}
	if !force {
		loc, err := time.LoadLocation("America/Los_Angeles")
		if err != nil {
			loc = time.UTC
		}
		today := civil.DateOf(time.Now().In(loc))
		if !end.Before(today) {
			return result, fmt.Errorf("refusing today-or-future backfill without force=true")
		}
	}

	for i := 0; i < daysInclusive(start, end); i++ {
		updated, err := regenerateDailyAgencyKPI(ctx, start.AddDays(i))
		if err != nil {
			return result, err
		}
		if updated {
			result.DailyUpdated++
		} else {
			result.DailyMissing++
		}
	}
	for weekEnd := weekEndForDate(start); !end.AddDays(6).Before(weekEnd); weekEnd = weekEnd.AddDays(7) {
		updated, err := regenerateWeeklyAgencyKPI(ctx, weekEnd)
		if err != nil {
			return result, err
		}
		if updated {
			result.WeeklyUpdated++
		} else {
			result.WeeklyMissing++
		}
	}
	for month := firstOfMonth(start); !firstOfMonth(end).Before(month); month = nextMonth(month) {
		if _, err := generateMonthlyStats(ctx, month); err != nil {
			return result, fmt.Errorf("generate month %s: %w", month, err)
		}
		result.MonthlyUpdated++
	}
	return result, nil
}

func regenerateDailyAgencyKPI(ctx context.Context, serviceDate civil.Date) (bool, error) {
	key := fmt.Sprintf("%s%s.json", statsArchivePrefix, serviceDate)
	body, exists, err := readObject(ctx, key)
	if err != nil || !exists {
		return false, err
	}
	var daily dailyStats
	if err := json.Unmarshal(body, &daily); err != nil {
		return false, fmt.Errorf("parse %s: %w", key, err)
	}
	gtfsBytes, err := readGTFSZipForServiceDate(ctx, serviceDate)
	if err != nil {
		return false, fmt.Errorf("read GTFS for %s: %w", serviceDate, err)
	}
	zr, err := zip.NewReader(bytes.NewReader(gtfsBytes), int64(len(gtfsBytes)))
	if err != nil {
		return false, err
	}
	activeServices, err := loadActiveServices(zr, serviceDate)
	if err != nil {
		return false, err
	}
	scheduledTripRoute, err := loadScheduledTripRoutes(zr, activeServices)
	if err != nil {
		return false, err
	}
	stopPlan, err := loadScheduledStopPlan(zr, scheduledTripRoute)
	if err != nil {
		return false, err
	}
	arrivalProgress, err := queryTripProgress(ctx, serviceDate)
	if err != nil {
		return false, err
	}
	system, routes, err := calculateDailyAgencyKPI(
		ctx,
		zr,
		serviceDate,
		scheduledTripRoute,
		stopPlan,
		arrivalProgress,
	)
	if err != nil {
		return false, err
	}
	daily.AgencyKPI = system
	seen := make(map[string]struct{}, len(daily.Routes))
	for i := range daily.Routes {
		routeID := daily.Routes[i].RouteID
		routeKPI, ok := routes[routeID]
		if !ok {
			routeKPI = emptyAgencyKPIStats()
		}
		daily.Routes[i].AgencyKPI = routeKPI
		seen[routeID] = struct{}{}
	}
	colors, err := loadRouteColors(zr)
	if err != nil {
		return false, err
	}
	for routeID, stats := range routes {
		if _, ok := seen[routeID]; ok {
			continue
		}
		color := colors[routeID]
		daily.Routes = append(daily.Routes, routeStats{
			RouteID:   routeID,
			AgencyKPI: stats,
			Color:     color.color,
			TextColor: color.text,
		})
	}
	daily.GeneratedAt = time.Now().UTC()
	payload, err := json.Marshal(&daily)
	if err != nil {
		return false, err
	}
	if err := writeObject(ctx, key, payload); err != nil {
		return false, err
	}
	updateLatest, err := isAtLeastAsRecentAsLatest(ctx, serviceDate)
	if err != nil {
		return false, err
	}
	if updateLatest {
		if err := writeObject(ctx, statsLatestKey, payload); err != nil {
			return false, err
		}
	}
	return true, nil
}

func regenerateWeeklyAgencyKPI(ctx context.Context, weekEnd civil.Date) (bool, error) {
	key := fmt.Sprintf("%s%s.json", weeklyArchivePrefix, weekEnd)
	body, exists, err := readObject(ctx, key)
	if err != nil || !exists {
		return false, err
	}
	var weekly weeklyStats
	if err := json.Unmarshal(body, &weekly); err != nil {
		return false, fmt.Errorf("parse %s: %w", key, err)
	}
	weekStart := weekEnd.AddDays(-6)
	dailies, err := readDailyStatsForWeek(ctx, weekStart)
	if err != nil {
		return false, err
	}
	var systemValues []agencyKPIStats
	routeValues := make(map[string][]agencyKPIStats)
	for _, daily := range dailies {
		if daily == nil {
			continue
		}
		systemValues = append(systemValues, daily.AgencyKPI)
		for _, route := range daily.Routes {
			routeValues[route.RouteID] = append(routeValues[route.RouteID], route.AgencyKPI)
		}
	}
	weekly.AgencyKPI = aggregateAgencyKPIStats(systemValues)
	for i := range weekly.RouteDailyServiceDelivered {
		routeID := weekly.RouteDailyServiceDelivered[i].RouteID
		weekly.RouteDailyServiceDelivered[i].AgencyKPI = aggregateAgencyKPIStats(routeValues[routeID])
		delete(routeValues, routeID)
	}
	for routeID, values := range routeValues {
		weekly.RouteDailyServiceDelivered = append(weekly.RouteDailyServiceDelivered, routeDailySD{
			RouteID:   routeID,
			AgencyKPI: aggregateAgencyKPIStats(values),
		})
	}
	weekly.GeneratedAt = time.Now().UTC()
	payload, err := json.Marshal(&weekly)
	if err != nil {
		return false, err
	}
	if err := writeObject(ctx, key, payload); err != nil {
		return false, err
	}
	updateLatest, err := isWeeklyAtLeastAsRecentAsLatest(ctx, weekEnd)
	if err != nil {
		return false, err
	}
	if updateLatest {
		if err := writeObject(ctx, weeklyLatestKey, payload); err != nil {
			return false, err
		}
	}
	return true, nil
}

func weekEndForDate(date civil.Date) civil.Date {
	daysUntilSaturday := (int(time.Saturday) - int(civilWeekday(date)) + 7) % 7
	return date.AddDays(daysUntilSaturday)
}

func firstOfMonth(date civil.Date) civil.Date {
	return civil.Date{Year: date.Year, Month: date.Month, Day: 1}
}

func nextMonth(date civil.Date) civil.Date {
	next := time.Date(date.Year, date.Month+1, 1, 0, 0, 0, 0, time.UTC)
	return civil.DateOf(next)
}

func previousPTDate(now time.Time) civil.Date {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	return civil.DateOf(now.In(loc)).AddDays(-1)
}
