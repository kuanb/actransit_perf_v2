package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/civil"
)

const maxBunchingBackfillDays = 14

type bunchingBackfillResult struct {
	StartDate       string   `json:"start_date"`
	EndDate         string   `json:"end_date"`
	DailyUpdated    int      `json:"daily_updated"`
	DailySkipped    int      `json:"daily_skipped"`
	DailyMissing    int      `json:"daily_missing"`
	WeeklyUpdated   int      `json:"weekly_updated"`
	WeeklyMissing   int      `json:"weekly_missing"`
	UpdatedDates    []string `json:"updated_dates"`
	UpdatedWeekEnds []string `json:"updated_week_ends"`
}

func processBunchingBackfill(ctx context.Context, start, end civil.Date, force bool) (*bunchingBackfillResult, error) {
	if end.Before(start) {
		return nil, fmt.Errorf("end_date must be on or after start_date")
	}
	days := 1
	for d := start; d.Before(end); d = d.AddDays(1) {
		days++
	}
	if days > maxBunchingBackfillDays {
		return nil, fmt.Errorf("range is %d days; maximum is %d", days, maxBunchingBackfillDays)
	}

	result := &bunchingBackfillResult{StartDate: start.String(), EndDate: end.String()}
	weekEnds := make(map[civil.Date]struct{})
	for date := start; !end.Before(date); date = date.AddDays(1) {
		updated, exists, err := backfillDailyBunching(ctx, date, force)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", date, err)
		}
		if !exists {
			result.DailyMissing++
			continue
		}
		weekEnds[weekEndSaturdayForDate(date)] = struct{}{}
		if updated {
			result.DailyUpdated++
			result.UpdatedDates = append(result.UpdatedDates, date.String())
		} else {
			result.DailySkipped++
		}
	}

	orderedWeekEnds := make([]civil.Date, 0, len(weekEnds))
	for weekEnd := range weekEnds {
		orderedWeekEnds = append(orderedWeekEnds, weekEnd)
	}
	sort.Slice(orderedWeekEnds, func(i, j int) bool { return orderedWeekEnds[i].Before(orderedWeekEnds[j]) })
	for _, weekEnd := range orderedWeekEnds {
		updated, exists, err := backfillWeeklyBunching(ctx, weekEnd)
		if err != nil {
			return nil, fmt.Errorf("week %s: %w", weekEnd, err)
		}
		if !exists {
			result.WeeklyMissing++
			continue
		}
		if updated {
			result.WeeklyUpdated++
			result.UpdatedWeekEnds = append(result.UpdatedWeekEnds, weekEnd.String())
		}
	}
	return result, nil
}

func backfillDailyBunching(ctx context.Context, serviceDate civil.Date, force bool) (bool, bool, error) {
	key := fmt.Sprintf("%s%s.json", statsArchivePrefix, serviceDate)
	body, exists, err := readObject(ctx, key)
	if err != nil || !exists {
		return false, exists, err
	}
	var stats dailyStats
	if err := json.Unmarshal(body, &stats); err != nil {
		return false, true, fmt.Errorf("parse %s: %w", key, err)
	}
	if !force && dailyHasCurrentBunching(&stats) {
		return false, true, nil
	}

	gtfsBytes, err := readGTFSZipForServiceDate(ctx, serviceDate)
	if err != nil {
		return false, true, fmt.Errorf("read gtfs zip: %w", err)
	}
	zr, err := zip.NewReader(bytes.NewReader(gtfsBytes), int64(len(gtfsBytes)))
	if err != nil {
		return false, true, fmt.Errorf("open gtfs zip: %w", err)
	}
	services, err := loadActiveServices(zr, serviceDate)
	if err != nil {
		return false, true, fmt.Errorf("active services: %w", err)
	}
	system, routes, err := calculateDailyBunching(ctx, zr, serviceDate, services)
	if err != nil {
		return false, true, err
	}
	stats.MethodologyVersion = 3
	stats.System.Bunching = system
	generatedAt := time.Now().UTC()
	for i := range stats.Routes {
		stats.Routes[i].Bunching = routes[stats.Routes[i].RouteID]
		if stats.Routes[i].Bunching == nil {
			stats.Routes[i].Bunching = finalizeBunchingStats(bunchingAccumulator{}, nil, nil, generatedAt, 1, 1)
		}
	}
	payload, err := json.MarshalIndent(&stats, "", "  ")
	if err != nil {
		return false, true, fmt.Errorf("marshal: %w", err)
	}
	if err := writeObject(ctx, key, payload); err != nil {
		return false, true, fmt.Errorf("write %s: %w", key, err)
	}
	if err := updateDailyLatestBunching(ctx, serviceDate, payload); err != nil {
		return false, true, err
	}
	return true, true, nil
}

func dailyHasCurrentBunching(stats *dailyStats) bool {
	if stats.System.Bunching == nil || stats.System.Bunching.MethodologyVersion != bunchingMethodologyVersion {
		return false
	}
	for i := range stats.Routes {
		if stats.Routes[i].Bunching == nil || stats.Routes[i].Bunching.MethodologyVersion != bunchingMethodologyVersion {
			return false
		}
	}
	return true
}

func updateDailyLatestBunching(ctx context.Context, serviceDate civil.Date, payload []byte) error {
	body, exists, err := readObject(ctx, statsLatestKey)
	if err != nil || !exists {
		return err
	}
	var latest dailyStats
	if err := json.Unmarshal(body, &latest); err != nil {
		return fmt.Errorf("parse %s: %w", statsLatestKey, err)
	}
	if latest.ServiceDate != serviceDate.String() {
		return nil
	}
	if err := writeObject(ctx, statsLatestKey, payload); err != nil {
		return fmt.Errorf("write %s: %w", statsLatestKey, err)
	}
	return nil
}

func weekEndSaturdayForDate(date civil.Date) civil.Date {
	daysForward := (int(time.Saturday) - int(civilWeekday(date)) + 7) % 7
	return date.AddDays(daysForward)
}

func backfillWeeklyBunching(ctx context.Context, weekEnd civil.Date) (bool, bool, error) {
	key := fmt.Sprintf("%s%s.json", weeklyArchivePrefix, weekEnd)
	body, exists, err := readObject(ctx, key)
	if err != nil || !exists {
		return false, exists, err
	}
	var stats weeklyStats
	if err := json.Unmarshal(body, &stats); err != nil {
		return false, true, fmt.Errorf("parse %s: %w", key, err)
	}
	dailies, err := readDailyStatsForWeek(ctx, weekEnd.AddDays(-6))
	if err != nil {
		return false, true, err
	}
	stats.MethodologyVersion = 3
	if stats.System != nil {
		values := make([]*bunchingStats, 7)
		for i, daily := range dailies {
			if daily != nil {
				values[i] = daily.System.Bunching
			}
		}
		stats.System.Bunching = aggregateBunchingStats(values, 7)
	}
	for i := range stats.RouteDailyServiceDelivered {
		values := make([]*bunchingStats, 7)
		activeDays := 0
		for day, daily := range dailies {
			if daily == nil {
				continue
			}
			for route := range daily.Routes {
				if daily.Routes[route].RouteID == stats.RouteDailyServiceDelivered[i].RouteID {
					values[day] = daily.Routes[route].Bunching
					activeDays++
					break
				}
			}
		}
		stats.RouteDailyServiceDelivered[i].Bunching = aggregateBunchingStats(values, activeDays)
	}
	payload, err := json.MarshalIndent(&stats, "", "  ")
	if err != nil {
		return false, true, fmt.Errorf("marshal: %w", err)
	}
	if err := writeObject(ctx, key, payload); err != nil {
		return false, true, fmt.Errorf("write %s: %w", key, err)
	}
	if err := updateWeeklyLatestBunching(ctx, weekEnd, payload); err != nil {
		return false, true, err
	}
	return true, true, nil
}

func updateWeeklyLatestBunching(ctx context.Context, weekEnd civil.Date, payload []byte) error {
	body, exists, err := readObject(ctx, weeklyLatestKey)
	if err != nil || !exists {
		return err
	}
	var latest weeklyStats
	if err := json.Unmarshal(body, &latest); err != nil {
		return fmt.Errorf("parse %s: %w", weeklyLatestKey, err)
	}
	if latest.WeekEnd != weekEnd.String() {
		return nil
	}
	if err := writeObject(ctx, weeklyLatestKey, payload); err != nil {
		return fmt.Errorf("write %s: %w", weeklyLatestKey, err)
	}
	return nil
}
