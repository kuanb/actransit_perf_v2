package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	weeklyArchivePrefix        = "stats/weekly/"
	weeklyLatestKey            = "stats/weekly/latest.json"
	weeklyIndexKey             = "stats/weekly/_index.json"
	minObservationsForHourCell = 10
)

// dayNames is indexed 0..6 for Sun..Sat. Matches BQ DAYOFWEEK semantics
// (1=Sun..7=Sat) shifted by -1, and matches the iteration order of an
// inclusive Sunday→Saturday week (weekStart.AddDays(0..6)).
var dayNames = [7]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}

type weeklyStats struct {
	WeekStart                  string                  `json:"week_start"`
	WeekEnd                    string                  `json:"week_end"`
	GeneratedAt                time.Time               `json:"generated_at"`
	DailyServiceDelivered      []dailyServiceDelivered `json:"daily_service_delivered"`
	SystemDelayHeatmap         map[string][]delayCell  `json:"system_delay_heatmap"`
	RouteDelayByHour           map[string][]delayCell  `json:"route_delay_by_hour"`
	RouteDailyServiceDelivered []routeDailySD          `json:"route_daily_service_delivered"`
}

type dailyServiceDelivered struct {
	Day         string  `json:"day"`
	ServiceDate string  `json:"service_date"`
	Scheduled   int     `json:"scheduled"`
	Ran         int     `json:"ran"`
	Pct         float64 `json:"pct"`
}

// delayCell is one (day, hour) bucket in the system heatmap or one hour
// bucket in a route's hourly series. P50 / P95 are signed minutes (early
// is negative). Both are nil when N is below minObservationsForHourCell
// or when the bucket had no data.
type delayCell struct {
	Hour int      `json:"hour"`
	P50  *float64 `json:"p50"`
	P95  *float64 `json:"p95"`
	N    int64    `json:"n"`
}

type routeDailySD struct {
	RouteID            string              `json:"route_id"`
	OverallP50DelayMin *float64            `json:"overall_p50_delay_min"`
	ByDay              []routeDailySDByDay `json:"by_day"`
	Color              string              `json:"color"`
	TextColor          string              `json:"text_color"`
}

type routeDailySDByDay struct {
	Day         string   `json:"day"`
	ServiceDate string   `json:"service_date"`
	Pct         *float64 `json:"pct"`
}

// processWeeklyStats reads the 7 already-computed daily stats files for
// the week ending on weekEndSat (a Saturday), runs three BigQuery
// queries for delay heatmap + per-route hourly + per-route overall p50,
// and writes stats/weekly/<sat>.json + latest + index. The route grid
// is sorted worst→best by week-overall p50 delay.
func processWeeklyStats(ctx context.Context, weekEndSat civil.Date) (*weeklyStats, error) {
	if civilWeekday(weekEndSat) != time.Saturday {
		return nil, fmt.Errorf("week_end must be a Saturday; got %s (%s)", weekEndSat, civilWeekday(weekEndSat))
	}
	weekStart := weekEndSat.AddDays(-6)

	out := &weeklyStats{
		WeekStart:   weekStart.String(),
		WeekEnd:     weekEndSat.String(),
		GeneratedAt: time.Now().UTC(),
	}

	dailies, err := readDailyStatsForWeek(ctx, weekStart)
	if err != nil {
		return nil, fmt.Errorf("read daily stats: %w", err)
	}
	out.DailyServiceDelivered = aggregateDailyServiceDelivered(dailies, weekStart)

	sysHeatmap, err := queryWeeklySystemDelayHeatmap(ctx, weekStart, weekEndSat)
	if err != nil {
		return nil, fmt.Errorf("query system heatmap: %w", err)
	}
	out.SystemDelayHeatmap = sysHeatmap

	routeHourly, err := queryWeeklyRouteDelayByHour(ctx, weekStart, weekEndSat)
	if err != nil {
		return nil, fmt.Errorf("query route hourly: %w", err)
	}
	out.RouteDelayByHour = routeHourly

	routeOverall, err := queryWeeklyRouteOverallDelay(ctx, weekStart, weekEndSat)
	if err != nil {
		return nil, fmt.Errorf("query route overall: %w", err)
	}
	out.RouteDailyServiceDelivered = aggregateRouteDailySD(dailies, weekStart, routeOverall)

	payload, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	updateLatest, err := isWeeklyAtLeastAsRecentAsLatest(ctx, weekEndSat)
	if err != nil {
		slog.Warn("compare to existing weekly latest failed; writing anyway", "err", err)
		updateLatest = true
	}
	if updateLatest {
		if err := writeObject(ctx, weeklyLatestKey, payload); err != nil {
			return nil, fmt.Errorf("write weekly latest: %w", err)
		}
	} else {
		slog.Info("generate-weekly-stats skipped latest write",
			"week_end", weekEndSat.String(),
			"reason", "older than current weekly/latest.json",
		)
	}

	archiveKey := fmt.Sprintf("%s%s.json", weeklyArchivePrefix, weekEndSat.String())
	if err := writeObject(ctx, archiveKey, payload); err != nil {
		return nil, fmt.Errorf("write weekly archive %s: %w", archiveKey, err)
	}
	if err := updateWeeklyIndex(ctx, weekEndSat.String()); err != nil {
		return out, fmt.Errorf("update weekly index: %w", err)
	}
	return out, nil
}

// civilWeekday returns the time.Weekday for a civil.Date (interpreting
// the date as midnight UTC, which is unambiguous for weekday lookup).
func civilWeekday(d civil.Date) time.Weekday {
	return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.UTC).Weekday()
}

// defaultWeekEndSaturday returns the most recent Saturday strictly
// before today in PT — i.e. "the Saturday whose week is fully written."
// Cloud Scheduler fires this job at 03:00 PT on Sunday, by which time
// the daily auto-rollup for the prior Saturday has finished.
func defaultWeekEndSaturday(now time.Time) civil.Date {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		loc = time.UTC
	}
	today := civil.DateOf(now.In(loc))
	// Days back to the most recent Saturday strictly earlier than today.
	// Sun (wd=0) → 1, Mon (1) → 2, ..., Fri (5) → 6, Sat (6) → 7 (skip
	// today's not-yet-complete Saturday and go to last week's).
	daysBack := int(civilWeekday(today)) + 1
	return today.AddDays(-daysBack)
}

func readDailyStatsForWeek(ctx context.Context, weekStart civil.Date) ([]*dailyStats, error) {
	out := make([]*dailyStats, 7)
	for i := 0; i < 7; i++ {
		d := weekStart.AddDays(i)
		key := fmt.Sprintf("%s%s.json", statsArchivePrefix, d.String())
		body, exists, err := readObject(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", key, err)
		}
		if !exists {
			slog.Warn("daily stats missing for weekly aggregation",
				"service_date", d.String(), "key", key)
			continue
		}
		var ds dailyStats
		if err := json.Unmarshal(body, &ds); err != nil {
			return nil, fmt.Errorf("parse %s: %w", key, err)
		}
		out[i] = &ds
	}
	return out, nil
}

func aggregateDailyServiceDelivered(dailies []*dailyStats, weekStart civil.Date) []dailyServiceDelivered {
	out := make([]dailyServiceDelivered, 0, 7)
	for i := 0; i < 7; i++ {
		d := weekStart.AddDays(i)
		entry := dailyServiceDelivered{
			Day:         dayNames[i],
			ServiceDate: d.String(),
		}
		if dailies[i] != nil {
			sc := dailies[i].ScheduleCompliance
			entry.Scheduled = sc.ScheduledTrips
			entry.Ran = sc.RanTrips
			if entry.Scheduled > 0 {
				entry.Pct = round1(100 * float64(entry.Ran) / float64(entry.Scheduled))
			}
		}
		out = append(out, entry)
	}
	return out
}

// aggregateRouteDailySD pivots the per-day routes lists into a per-route
// matrix [route_id][day_idx] of SD%, attaches each route's overall p50
// delay (in seconds, from BQ) for sorting, and sorts worst→best so the
// frontend can render the grid top-down without re-sorting.
func aggregateRouteDailySD(dailies []*dailyStats, weekStart civil.Date, routeOverallSec map[string]float64) []routeDailySD {
	type accum struct {
		rid       string
		color     string
		textColor string
		byDay     [7]*float64
	}
	byRoute := make(map[string]*accum)

	for i := 0; i < 7; i++ {
		ds := dailies[i]
		if ds == nil {
			continue
		}
		for _, r := range ds.Routes {
			a, ok := byRoute[r.RouteID]
			if !ok {
				a = &accum{rid: r.RouteID, color: r.Color, textColor: r.TextColor}
				byRoute[r.RouteID] = a
			}
			if a.color == "" && r.Color != "" {
				a.color = r.Color
			}
			if a.textColor == "" && r.TextColor != "" {
				a.textColor = r.TextColor
			}
			if r.ServiceDeliveredPct != nil {
				v := *r.ServiceDeliveredPct
				a.byDay[i] = &v
			}
		}
	}

	type sortItem struct {
		a       *accum
		p50Sec  float64
		hasP50  bool
	}
	items := make([]sortItem, 0, len(byRoute))
	for rid, a := range byRoute {
		s := sortItem{a: a}
		if v, ok := routeOverallSec[rid]; ok {
			s.p50Sec = v
			s.hasP50 = true
		}
		items = append(items, s)
	}
	sort.Slice(items, func(i, j int) bool {
		// Routes without p50 sink to the bottom; among those that have
		// p50, larger (worse) is first.
		if !items[i].hasP50 && !items[j].hasP50 {
			return items[i].a.rid < items[j].a.rid
		}
		if !items[i].hasP50 {
			return false
		}
		if !items[j].hasP50 {
			return true
		}
		return items[i].p50Sec > items[j].p50Sec
	})

	out := make([]routeDailySD, 0, len(items))
	for _, it := range items {
		byDay := make([]routeDailySDByDay, 7)
		for i := 0; i < 7; i++ {
			byDay[i] = routeDailySDByDay{
				Day:         dayNames[i],
				ServiceDate: weekStart.AddDays(i).String(),
				Pct:         it.a.byDay[i],
			}
		}
		var p50Min *float64
		if it.hasP50 {
			v := round1(it.p50Sec / 60.0)
			p50Min = &v
		}
		out = append(out, routeDailySD{
			RouteID:            it.a.rid,
			OverallP50DelayMin: p50Min,
			ByDay:              byDay,
			Color:              it.a.color,
			TextColor:          it.a.textColor,
		})
	}
	return out
}

func queryWeeklySystemDelayHeatmap(ctx context.Context, weekStart, weekEnd civil.Date) (map[string][]delayCell, error) {
	q := bqClient.Query(fmt.Sprintf(`
		SELECT
		  EXTRACT(DAYOFWEEK FROM service_date) AS dow,
		  EXTRACT(HOUR FROM actual_arrival AT TIME ZONE "America/Los_Angeles") AS hour,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_sec,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_sec,
		  COUNT(*) AS n
		FROM `+"`%s.actransit.trip_observations`"+`
		WHERE service_date BETWEEN "%s" AND "%s"
		  AND actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY dow, hour
		ORDER BY dow, hour
	`, projectID, weekStart.String(), weekEnd.String()))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]delayCell, 7)
	for i := 0; i < 7; i++ {
		cells := make([]delayCell, 24)
		for h := 0; h < 24; h++ {
			cells[h] = delayCell{Hour: h}
		}
		out[dayNames[i]] = cells
	}

	for {
		var row struct {
			DOW    int64              `bigquery:"dow"`
			Hour   int64              `bigquery:"hour"`
			P50Sec bigquery.NullInt64 `bigquery:"p50_sec"`
			P95Sec bigquery.NullInt64 `bigquery:"p95_sec"`
			N      int64              `bigquery:"n"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		// BQ DAYOFWEEK: 1=Sunday..7=Saturday → index 0..6.
		dayIdx := int(row.DOW) - 1
		if dayIdx < 0 || dayIdx >= 7 || row.Hour < 0 || row.Hour >= 24 {
			continue
		}
		cell := delayCell{Hour: int(row.Hour), N: row.N}
		if row.N >= minObservationsForHourCell {
			if row.P50Sec.Valid {
				v := round1(float64(row.P50Sec.Int64) / 60.0)
				cell.P50 = &v
			}
			if row.P95Sec.Valid {
				v := round1(float64(row.P95Sec.Int64) / 60.0)
				cell.P95 = &v
			}
		}
		out[dayNames[dayIdx]][int(row.Hour)] = cell
	}
	return out, nil
}

func queryWeeklyRouteDelayByHour(ctx context.Context, weekStart, weekEnd civil.Date) (map[string][]delayCell, error) {
	q := bqClient.Query(fmt.Sprintf(`
		SELECT
		  route_id,
		  EXTRACT(HOUR FROM actual_arrival AT TIME ZONE "America/Los_Angeles") AS hour,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_sec,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_sec,
		  COUNT(*) AS n
		FROM `+"`%s.actransit.trip_observations`"+`
		WHERE service_date BETWEEN "%s" AND "%s"
		  AND actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY route_id, hour
		HAVING n >= %d
		ORDER BY route_id, hour
	`, projectID, weekStart.String(), weekEnd.String(), minObservationsForHourCell))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	out := make(map[string][]delayCell)
	for {
		var row struct {
			RouteID string             `bigquery:"route_id"`
			Hour    int64              `bigquery:"hour"`
			P50Sec  bigquery.NullInt64 `bigquery:"p50_sec"`
			P95Sec  bigquery.NullInt64 `bigquery:"p95_sec"`
			N       int64              `bigquery:"n"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		cell := delayCell{Hour: int(row.Hour), N: row.N}
		if row.P50Sec.Valid {
			v := round1(float64(row.P50Sec.Int64) / 60.0)
			cell.P50 = &v
		}
		if row.P95Sec.Valid {
			v := round1(float64(row.P95Sec.Int64) / 60.0)
			cell.P95 = &v
		}
		out[row.RouteID] = append(out[row.RouteID], cell)
	}
	return out, nil
}

// queryWeeklyRouteOverallDelay returns the per-route p50 of delay_seconds
// across the whole week (in seconds, kept at sec precision for sort
// stability). Used to sort the route grid worst→best.
func queryWeeklyRouteOverallDelay(ctx context.Context, weekStart, weekEnd civil.Date) (map[string]float64, error) {
	q := bqClient.Query(fmt.Sprintf(`
		SELECT
		  route_id,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_sec,
		  COUNT(*) AS n
		FROM `+"`%s.actransit.trip_observations`"+`
		WHERE service_date BETWEEN "%s" AND "%s"
		  AND actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY route_id
		HAVING n >= %d
	`, projectID, weekStart.String(), weekEnd.String(), minObservationsForHourCell))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64)
	for {
		var row struct {
			RouteID string             `bigquery:"route_id"`
			P50Sec  bigquery.NullInt64 `bigquery:"p50_sec"`
			N       int64              `bigquery:"n"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if row.P50Sec.Valid {
			out[row.RouteID] = float64(row.P50Sec.Int64)
		}
	}
	return out, nil
}

// isWeeklyAtLeastAsRecentAsLatest mirrors isAtLeastAsRecentAsLatest's
// semantics for the weekly latest pointer. Same rationale: a backfill
// of an older week shouldn't clobber a newer week's "latest" view.
func isWeeklyAtLeastAsRecentAsLatest(ctx context.Context, candidate civil.Date) (bool, error) {
	body, exists, err := readObject(ctx, weeklyLatestKey)
	if err != nil {
		return false, err
	}
	if !exists {
		return true, nil
	}
	var existing weeklyStats
	if err := json.Unmarshal(body, &existing); err != nil {
		return false, fmt.Errorf("parse existing weekly latest: %w", err)
	}
	if existing.WeekEnd == "" {
		return true, nil
	}
	existingDate, err := civil.ParseDate(existing.WeekEnd)
	if err != nil {
		return false, fmt.Errorf("parse existing week_end %q: %w", existing.WeekEnd, err)
	}
	return !candidate.Before(existingDate), nil
}

type weeklyIndex struct {
	Weeks     []string  `json:"weeks"`
	UpdatedAt time.Time `json:"updated_at"`
}

func updateWeeklyIndex(ctx context.Context, weekEndStr string) error {
	idx := weeklyIndex{}
	body, exists, err := readObject(ctx, weeklyIndexKey)
	if err == nil && exists {
		_ = json.Unmarshal(body, &idx)
	}
	for _, w := range idx.Weeks {
		if w == weekEndStr {
			idx.UpdatedAt = time.Now().UTC()
			payload, _ := json.MarshalIndent(idx, "", "  ")
			return writeObject(ctx, weeklyIndexKey, payload)
		}
	}
	idx.Weeks = append(idx.Weeks, weekEndStr)
	sort.Sort(sort.Reverse(sort.StringSlice(idx.Weeks)))
	idx.UpdatedAt = time.Now().UTC()
	payload, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return writeObject(ctx, weeklyIndexKey, payload)
}
