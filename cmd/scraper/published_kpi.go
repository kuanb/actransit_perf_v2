package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const (
	publishedKPIServiceOperatedURL = "https://kpitest.actransit.org/api/KPIChart/GetChartById?Id=120-3"
	publishedKPIOnTimeURL          = "https://kpitest.actransit.org/api/KPIChart/GetChartById?Id=10"
	publishedKPILatestKey          = "stats/published-kpis/latest.json"
	publishedKPISnapshotPrefix     = "stats/published-kpis/snapshots/"
)

var fiscalYearPattern = regexp.MustCompile(`^FY ([0-9]{2})-([0-9]{2})$`)

type publishedKPIStats struct {
	FetchedAt         time.Time           `json:"fetched_at"`
	Source            string              `json:"source"`
	ServiceOperated   []publishedKPIValue `json:"service_operated"`
	OnTimePerformance []publishedKPIValue `json:"on_time_performance"`
	Raw               publishedKPIRaw     `json:"raw"`
}

type publishedKPIValue struct {
	Month     string   `json:"month"`
	Pct       float64  `json:"pct"`
	TargetPct *float64 `json:"target_pct,omitempty"`
}

type publishedKPIRaw struct {
	ServiceOperated   json.RawMessage `json:"service_operated"`
	OnTimePerformance json.RawMessage `json:"on_time_performance"`
}

type publishedChart struct {
	ID   string                       `json:"Id"`
	Data []map[string]json.RawMessage `json:"Data"`
}

func refreshPublishedKPIs(ctx context.Context) (*publishedKPIStats, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	serviceOperated, serviceRaw, err := fetchPublishedChart(ctx, client, publishedKPIServiceOperatedURL)
	if err != nil {
		return nil, fmt.Errorf("fetch Service Operated: %w", err)
	}
	onTime, onTimeRaw, err := fetchPublishedChart(ctx, client, publishedKPIOnTimeURL)
	if err != nil {
		return nil, fmt.Errorf("fetch On Time Performance: %w", err)
	}
	serviceValues, err := normalizePublishedServiceOperated(serviceOperated)
	if err != nil {
		return nil, err
	}
	onTimeValues, err := normalizePublishedOnTime(onTime)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := &publishedKPIStats{
		FetchedAt:         now,
		Source:            "https://kpitest.actransit.org/",
		ServiceOperated:   serviceValues,
		OnTimePerformance: onTimeValues,
		Raw: publishedKPIRaw{
			ServiceOperated:   serviceRaw,
			OnTimePerformance: onTimeRaw,
		},
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	snapshotKey := fmt.Sprintf("%s%s.json", publishedKPISnapshotPrefix, now.Format("2006-01-02"))
	if err := writeObject(ctx, snapshotKey, payload); err != nil {
		return nil, fmt.Errorf("write snapshot: %w", err)
	}
	if err := writeObject(ctx, publishedKPILatestKey, payload); err != nil {
		return nil, fmt.Errorf("write latest: %w", err)
	}
	return out, nil
}

func fetchPublishedChart(ctx context.Context, client *http.Client, url string) (publishedChart, json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return publishedChart{}, nil, err
	}
	req.Header.Set("User-Agent", "actransit-performance-comparison/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return publishedChart{}, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return publishedChart{}, nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return publishedChart{}, nil, err
	}
	var chart publishedChart
	if err := json.Unmarshal(body, &chart); err != nil {
		return publishedChart{}, nil, err
	}
	if chart.ID == "" || len(chart.Data) == 0 {
		return publishedChart{}, nil, fmt.Errorf("chart response has no data")
	}
	return chart, json.RawMessage(body), nil
}

func normalizePublishedServiceOperated(chart publishedChart) ([]publishedKPIValue, error) {
	if chart.ID != "120-3" {
		return nil, fmt.Errorf("unexpected Service Operated chart ID %q", chart.ID)
	}
	values := make([]publishedKPIValue, 0, len(chart.Data))
	seen := make(map[string]struct{})
	for _, row := range chart.Data {
		month, err := chartMonth(row["REPORTFROMDATE"])
		if err != nil {
			return nil, fmt.Errorf("Service Operated month: %w", err)
		}
		pct, err := chartFloat(row["Trips Operated"])
		if err != nil {
			return nil, fmt.Errorf("Service Operated %s: %w", month, err)
		}
		if _, ok := seen[month]; ok {
			return nil, fmt.Errorf("duplicate Service Operated month %s", month)
		}
		seen[month] = struct{}{}
		value := publishedKPIValue{Month: month, Pct: pct}
		target, hasTarget, err := optionalChartFloat(row["Target"])
		if err != nil {
			return nil, fmt.Errorf("Service Operated %s target: %w", month, err)
		}
		if hasTarget {
			value.TargetPct = &target
		}
		if err := validatePublishedKPI(value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Month < values[j].Month })
	return values, nil
}

func normalizePublishedOnTime(chart publishedChart) ([]publishedKPIValue, error) {
	if chart.ID != "10" {
		return nil, fmt.Errorf("unexpected On Time Performance chart ID %q", chart.ID)
	}
	values := make([]publishedKPIValue, 0, len(chart.Data)*4)
	seen := make(map[string]struct{})
	for _, row := range chart.Data {
		monthNumber, err := chartMonthNumber(row["Month"])
		if err != nil {
			return nil, fmt.Errorf("On Time Performance month: %w", err)
		}
		for key, raw := range row {
			match := fiscalYearPattern.FindStringSubmatch(key)
			if match == nil {
				continue
			}
			pct, ok, err := optionalChartFloat(raw)
			if err != nil {
				return nil, fmt.Errorf("On Time Performance %s: %w", key, err)
			}
			if !ok {
				continue
			}
			startYear, _ := strconv.Atoi(match[1])
			endYear, _ := strconv.Atoi(match[2])
			year := 2000 + startYear
			if monthNumber <= 6 {
				year = 2000 + endYear
			}
			month := fmt.Sprintf("%04d-%02d", year, monthNumber)
			if _, ok := seen[month]; ok {
				return nil, fmt.Errorf("duplicate On Time Performance month %s", month)
			}
			seen[month] = struct{}{}
			value := publishedKPIValue{Month: month, Pct: pct}
			target, hasTarget, err := optionalChartFloat(row["Target"])
			if err != nil {
				return nil, fmt.Errorf("On Time Performance %s target: %w", month, err)
			}
			if hasTarget {
				value.TargetPct = &target
			}
			if err := validatePublishedKPI(value); err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Month < values[j].Month })
	return values, nil
}

func chartMonth(raw json.RawMessage) (string, error) {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil || len(value) < 7 {
		return "", fmt.Errorf("invalid date")
	}
	if _, err := time.Parse("2006-01", value[:7]); err != nil {
		return "", err
	}
	return value[:7], nil
}

func chartMonthNumber(raw json.RawMessage) (int, error) {
	month, err := chartMonth(raw)
	if err != nil {
		return 0, err
	}
	value, _ := strconv.Atoi(month[5:7])
	return value, nil
}

func chartFloat(raw json.RawMessage) (float64, error) {
	var value float64
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return 0, fmt.Errorf("invalid number")
	}
	return value, nil
}

func optionalChartFloat(raw json.RawMessage) (float64, bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false, nil
	}
	value, err := chartFloat(raw)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

func validatePublishedKPI(value publishedKPIValue) error {
	if value.Pct < 0 || value.Pct > 100 {
		return fmt.Errorf("%s percentage %.2f is outside 0..100", value.Month, value.Pct)
	}
	if value.TargetPct != nil && (*value.TargetPct < 0 || *value.TargetPct > 100) {
		return fmt.Errorf("%s target %.2f is outside 0..100", value.Month, *value.TargetPct)
	}
	return nil
}
