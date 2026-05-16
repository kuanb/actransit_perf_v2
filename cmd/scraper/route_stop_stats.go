package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const routeStopWeeklyPrefix = "stats/weekly/route_stops/"

type routeStopWeeklyStat struct {
	N            int64    `json:"n"`
	P50DelayS    *float64 `json:"p50_delay_s"`
	P95DelayS    *float64 `json:"p95_delay_s"`
	StddevDelayS *float64 `json:"stddev_delay_s"`
}

type routeStopWeeklyStats struct {
	RouteID string                         `json:"route_id"`
	WeekEnd string                         `json:"week_end"`
	Stops   map[string]routeStopWeeklyStat `json:"stops"`
}

// generateAllRouteStopWeeklyStats queries BQ for per-(route, stop) delay
// percentiles and stddev over the full week, then writes one JSON file per
// route to stats/weekly/route_stops/<week_end>/<route_id>.json.
//
// A single BQ query covers all routes so we avoid N round-trips. The dedup
// CTE pattern is identical to the one used by the weekly system heatmap.
func generateAllRouteStopWeeklyStats(ctx context.Context, weekStart, weekEnd civil.Date) error {
	q := bqClient.Query(fmt.Sprintf(`
		WITH %s
		SELECT
		  route_id,
		  stop_id,
		  COUNT(*) AS n,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(50)] AS p50_delay_s,
		  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_delay_s,
		  STDDEV(CAST(delay_seconds AS FLOAT64)) AS stddev_delay_s
		FROM obs
		WHERE actual_arrival IS NOT NULL
		  AND is_stale = FALSE
		GROUP BY route_id, stop_id
		ORDER BY route_id, stop_id
	`, dedupedRangeObservationsCTE(weekStart, weekEnd)))

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("route stop weekly query: %w", err)
	}

	byRoute := make(map[string]map[string]routeStopWeeklyStat)
	for {
		var row struct {
			RouteID    string                `bigquery:"route_id"`
			StopID     string                `bigquery:"stop_id"`
			N          int64                 `bigquery:"n"`
			P50DelayS  bigquery.NullInt64    `bigquery:"p50_delay_s"`
			P95DelayS  bigquery.NullInt64    `bigquery:"p95_delay_s"`
			StddevDelayS bigquery.NullFloat64 `bigquery:"stddev_delay_s"`
		}
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("route stop weekly scan: %w", err)
		}
		if byRoute[row.RouteID] == nil {
			byRoute[row.RouteID] = make(map[string]routeStopWeeklyStat)
		}
		stat := routeStopWeeklyStat{N: row.N}
		if row.P50DelayS.Valid {
			v := float64(row.P50DelayS.Int64)
			stat.P50DelayS = &v
		}
		if row.P95DelayS.Valid {
			v := float64(row.P95DelayS.Int64)
			stat.P95DelayS = &v
		}
		if row.StddevDelayS.Valid {
			v := round1(row.StddevDelayS.Float64)
			stat.StddevDelayS = &v
		}
		byRoute[row.RouteID][row.StopID] = stat
	}

	weekEndStr := weekEnd.String()
	written := 0
	for routeID, stops := range byRoute {
		out := routeStopWeeklyStats{
			RouteID: routeID,
			WeekEnd: weekEndStr,
			Stops:   stops,
		}
		payload, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			slog.Warn("route stop weekly marshal failed", "route_id", routeID, "err", err)
			continue
		}
		key := fmt.Sprintf("%s%s/%s.json", routeStopWeeklyPrefix, weekEndStr, sanitizeRouteID(routeID))
		if err := writeObject(ctx, key, payload); err != nil {
			slog.Warn("route stop weekly write failed", "route_id", routeID, "key", key, "err", err)
			continue
		}
		written++
	}
	slog.Info("route stop weekly stats written", "week_end", weekEndStr, "routes", written)
	return nil
}
