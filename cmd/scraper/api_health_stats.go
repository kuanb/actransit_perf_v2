package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/iterator"
)

const (
	apiHealthLatestObjectKey = "stats/api-health/latest.json"
	apiHealthRollingWindow   = 7 * 24 * time.Hour
	apiHealthIngestionLag    = 2 * time.Minute
)

type apiHealthStats struct {
	PeriodStart       string                 `json:"period_start"`
	PeriodEnd         string                 `json:"period_end"`
	BucketGranularity string                 `json:"bucket_granularity"`
	WindowStart       *time.Time             `json:"window_start,omitempty"`
	WindowEnd         *time.Time             `json:"window_end,omitempty"`
	GeneratedAt       *time.Time             `json:"generated_at,omitempty"`
	Sources           []apiHealthSourceStats `json:"sources"`
}

type apiHealthSourceStats struct {
	Source                 string            `json:"source"`
	Requests               int64             `json:"requests"`
	SuccessfulRequests     int64             `json:"successful_requests"`
	SuccessPct             float64           `json:"success_pct"`
	P50LatencyMS           float64           `json:"p50_latency_ms"`
	P95LatencyMS           float64           `json:"p95_latency_ms"`
	P99LatencyMS           float64           `json:"p99_latency_ms"`
	MaxLatencyMS           float64           `json:"max_latency_ms"`
	TimeoutCount           int64             `json:"timeout_count"`
	HTTP4xxCount           int64             `json:"http_4xx_count"`
	HTTP5xxCount           int64             `json:"http_5xx_count"`
	OtherErrorCount        int64             `json:"other_error_count"`
	ExpectedPolls          int64             `json:"expected_polls"`
	SuccessfulPolls        int64             `json:"successful_polls"`
	PollAvailabilityPct    float64           `json:"poll_availability_pct"`
	LongestOutageMinutes   int64             `json:"longest_outage_minutes"`
	LongestOutageStartedAt *time.Time        `json:"longest_outage_started_at,omitempty"`
	LongestOutageEndedAt   *time.Time        `json:"longest_outage_ended_at,omitempty"`
	Buckets                []apiHealthBucket `json:"buckets"`
}

type apiHealthBucket struct {
	StartedAt          time.Time `json:"started_at"`
	Requests           int64     `json:"requests"`
	SuccessfulRequests int64     `json:"successful_requests"`
	SuccessPct         float64   `json:"success_pct"`
	P50LatencyMS       float64   `json:"p50_latency_ms"`
	P95LatencyMS       float64   `json:"p95_latency_ms"`
	P99LatencyMS       float64   `json:"p99_latency_ms"`
	MaxLatencyMS       float64   `json:"max_latency_ms"`
	TimeoutCount       int64     `json:"timeout_count"`
	HTTP4xxCount       int64     `json:"http_4xx_count"`
	HTTP5xxCount       int64     `json:"http_5xx_count"`
	OtherErrorCount    int64     `json:"other_error_count"`
}

type apiHealthAggregateRow struct {
	Source                 string                 `bigquery:"source"`
	IsTotal                bool                   `bigquery:"is_total"`
	HourStart              bigquery.NullTimestamp `bigquery:"hour_start"`
	Requests               int64                  `bigquery:"requests"`
	SuccessfulRequests     int64                  `bigquery:"successful_requests"`
	P50LatencyMS           bigquery.NullFloat64   `bigquery:"p50_latency_ms"`
	P95LatencyMS           bigquery.NullFloat64   `bigquery:"p95_latency_ms"`
	P99LatencyMS           bigquery.NullFloat64   `bigquery:"p99_latency_ms"`
	MaxLatencyMS           bigquery.NullFloat64   `bigquery:"max_latency_ms"`
	TimeoutCount           int64                  `bigquery:"timeout_count"`
	HTTP4xxCount           int64                  `bigquery:"http_4xx_count"`
	HTTP5xxCount           int64                  `bigquery:"http_5xx_count"`
	OtherErrorCount        int64                  `bigquery:"other_error_count"`
	ExpectedPolls          bigquery.NullInt64     `bigquery:"expected_polls"`
	SuccessfulPolls        bigquery.NullInt64     `bigquery:"successful_polls"`
	LongestOutageMinutes   bigquery.NullInt64     `bigquery:"longest_outage_minutes"`
	LongestOutageStartedAt bigquery.NullTimestamp `bigquery:"longest_outage_started_at"`
	LongestOutageEndedAt   bigquery.NullTimestamp `bigquery:"longest_outage_ended_at"`
}

func queryAPIHealthStats(ctx context.Context, periodStart, periodEnd civil.Date) (*apiHealthStats, error) {
	windowStart, windowEnd, err := apiHealthDateWindow(periodStart, periodEnd)
	if err != nil {
		return nil, err
	}
	stats, err := queryAPIHealthStatsWindow(ctx, periodStart, periodEnd, windowStart, windowEnd, false)
	if err != nil {
		return nil, err
	}
	if stats == nil && periodStart == periodEnd {
		return queryAPIHealthRollupStats(ctx, periodStart)
	}
	return stats, nil
}

func queryAPIHealthStatsWindow(ctx context.Context, periodStart, periodEnd civil.Date, windowStart, windowEnd time.Time, includeConfiguredSources bool) (*apiHealthStats, error) {
	q := bqClient.Query(fmt.Sprintf(`
		WITH requests AS (
		  SELECT
		    source,
		    observed_at,
		    latency_ms,
		    success,
		    outcome
		  FROM `+"`%s.%s.%s`"+`
		  WHERE service_date BETWEEN @period_start AND @period_end
		    AND observed_at >= @window_start
		    AND observed_at < @window_end
		  QUALIFY ROW_NUMBER() OVER (
		    PARTITION BY source, observed_at
		    ORDER BY ingested_at DESC
		  ) = 1
		), minute_results AS (
		  SELECT
		    source,
		    TIMESTAMP_TRUNC(observed_at, MINUTE) AS minute_start,
		    LOGICAL_OR(success) AS successful
		  FROM requests
		  GROUP BY source, minute_start
		), sources AS (
		  SELECT DISTINCT source FROM requests
		  UNION DISTINCT
		  SELECT source
		  FROM UNNEST(["vehicle_locations", "ridership"]) AS source
		  WHERE @include_configured_sources
		), expected_minutes AS (
		  SELECT source, minute_start
		  FROM sources
		  CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(
		    @window_start,
		    TIMESTAMP_SUB(@window_end, INTERVAL 1 MINUTE),
		    INTERVAL 1 MINUTE
		  )) AS minute_start
		), availability_minutes AS (
		  SELECT
		    expected.source,
		    expected.minute_start,
		    COALESCE(results.successful, FALSE) AS successful
		  FROM expected_minutes AS expected
		  LEFT JOIN minute_results AS results
		    USING (source, minute_start)
		), marked_minutes AS (
		  SELECT
		    *,
		    COUNTIF(successful) OVER (
		      PARTITION BY source
		      ORDER BY minute_start
		    ) AS outage_group
		  FROM availability_minutes
		), outage_runs AS (
		  SELECT
		    source,
		    COUNT(*) AS outage_minutes,
		    MIN(minute_start) AS outage_start,
		    TIMESTAMP_ADD(MAX(minute_start), INTERVAL 1 MINUTE) AS outage_end
		  FROM marked_minutes
		  WHERE NOT successful
		  GROUP BY source, outage_group
		), longest_outages AS (
		  SELECT
		    source,
		    ARRAY_AGG(
		      STRUCT(outage_minutes, outage_start, outage_end)
		      ORDER BY outage_minutes DESC, outage_start DESC
		      LIMIT 1
		    )[OFFSET(0)] AS outage
		  FROM outage_runs
		  GROUP BY source
		), availability AS (
		  SELECT
		    source,
		    COUNT(*) AS expected_polls,
		    COUNTIF(successful) AS successful_polls
		  FROM availability_minutes
		  GROUP BY source
		), request_totals AS (
		  SELECT
		    source,
		    COUNT(*) AS requests,
		    COUNTIF(success) AS successful_requests,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms,
		    MAX(latency_ms) AS max_latency_ms,
		    COUNTIF(outcome = "timeout") AS timeout_count,
		    COUNTIF(outcome = "http_4xx") AS http_4xx_count,
		    COUNTIF(outcome = "http_5xx") AS http_5xx_count,
		    COUNTIF(NOT success AND outcome NOT IN ("timeout", "http_4xx", "http_5xx")) AS other_error_count
		  FROM requests
		  GROUP BY source
		), aggregates AS (
		  SELECT
		    availability.source,
		    TRUE AS is_total,
		    CAST(NULL AS TIMESTAMP) AS hour_start,
		    COALESCE(totals.requests, 0) AS requests,
		    COALESCE(totals.successful_requests, 0) AS successful_requests,
		    totals.p50_latency_ms,
		    totals.p95_latency_ms,
		    totals.p99_latency_ms,
		    totals.max_latency_ms,
		    COALESCE(totals.timeout_count, 0) AS timeout_count,
		    COALESCE(totals.http_4xx_count, 0) AS http_4xx_count,
		    COALESCE(totals.http_5xx_count, 0) AS http_5xx_count,
		    COALESCE(totals.other_error_count, 0) AS other_error_count,
		    availability.expected_polls,
		    availability.successful_polls,
		    COALESCE(longest.outage.outage_minutes, 0) AS longest_outage_minutes,
		    longest.outage.outage_start AS longest_outage_started_at,
		    longest.outage.outage_end AS longest_outage_ended_at
		  FROM availability
		  LEFT JOIN request_totals AS totals USING (source)
		  LEFT JOIN longest_outages AS longest USING (source)

		  UNION ALL

		  SELECT
		    source,
		    FALSE AS is_total,
		    TIMESTAMP_TRUNC(observed_at, HOUR, "America/Los_Angeles") AS hour_start,
		    COUNT(*) AS requests,
		    COUNTIF(success) AS successful_requests,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms,
		    MAX(latency_ms) AS max_latency_ms,
		    COUNTIF(outcome = "timeout") AS timeout_count,
		    COUNTIF(outcome = "http_4xx") AS http_4xx_count,
		    COUNTIF(outcome = "http_5xx") AS http_5xx_count,
		    COUNTIF(NOT success AND outcome NOT IN ("timeout", "http_4xx", "http_5xx")) AS other_error_count,
		    CAST(NULL AS INT64) AS expected_polls,
		    CAST(NULL AS INT64) AS successful_polls,
		    CAST(NULL AS INT64) AS longest_outage_minutes,
		    CAST(NULL AS TIMESTAMP) AS longest_outage_started_at,
		    CAST(NULL AS TIMESTAMP) AS longest_outage_ended_at
		  FROM requests
		  GROUP BY source, hour_start
		)
		SELECT * FROM aggregates
		ORDER BY source, is_total DESC, hour_start
	`, projectID, bqDatasetID, apiRequestBQTable))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "period_start", Value: periodStart},
		{Name: "period_end", Value: periodEnd},
		{Name: "window_start", Value: windowStart},
		{Name: "window_end", Value: windowEnd},
		{Name: "include_configured_sources", Value: includeConfiguredSources},
	}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	var rows []apiHealthAggregateRow
	for {
		var row apiHealthAggregateRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	stats := assembleAPIHealthStats(periodStart, periodEnd, rows)
	if stats != nil {
		stats.WindowStart = &windowStart
		stats.WindowEnd = &windowEnd
	}
	return stats, nil
}

func apiHealthDateWindow(periodStart, periodEnd civil.Date) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("load Pacific time: %w", err)
	}
	start := time.Date(periodStart.Year, periodStart.Month, periodStart.Day, 0, 0, 0, 0, loc)
	endDate := periodEnd.AddDays(1)
	end := time.Date(endDate.Year, endDate.Month, endDate.Day, 0, 0, 0, 0, loc)
	return start, end, nil
}

func generateCurrentAPIHealth(ctx context.Context, now time.Time) (*apiHealthStats, error) {
	windowEnd := now.UTC().Add(-apiHealthIngestionLag).Truncate(time.Minute).Add(time.Minute)
	windowStart := windowEnd.Add(-apiHealthRollingWindow)
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		return nil, fmt.Errorf("load Pacific time: %w", err)
	}
	periodStart := civil.DateOf(windowStart.In(loc))
	periodEnd := civil.DateOf(windowEnd.Add(-time.Nanosecond).In(loc))
	stats, err := queryAPIHealthStatsWindow(ctx, periodStart, periodEnd, windowStart, windowEnd, true)
	if err != nil {
		return nil, err
	}
	if stats == nil {
		return nil, fmt.Errorf("no API request observations in rolling window")
	}
	generatedAt := now.UTC()
	stats.GeneratedAt = &generatedAt
	payload, err := json.Marshal(stats)
	if err != nil {
		return nil, fmt.Errorf("marshal current API health: %w", err)
	}
	if err := writeObject(ctx, apiHealthLatestObjectKey, payload); err != nil {
		return nil, fmt.Errorf("write current API health: %w", err)
	}
	return stats, nil
}

func rollupAPIHealthDay(ctx context.Context, serviceDate civil.Date) error {
	q := bqClient.Query(fmt.Sprintf(`
		MERGE `+"`%s.%s.%s`"+` AS target
		USING (
		  WITH requests AS (
		    SELECT source, observed_at, latency_ms, success, outcome
		    FROM `+"`%s.%s.%s`"+`
		    WHERE service_date = @service_date
		    QUALIFY ROW_NUMBER() OVER (
		      PARTITION BY source, observed_at
		      ORDER BY ingested_at DESC
		    ) = 1
		  )
		  SELECT
		    @service_date AS service_date,
		    source,
		    TRUE AS is_total,
		    CAST(NULL AS TIMESTAMP) AS hour_start,
		    COUNT(*) AS requests,
		    COUNTIF(success) AS successful_requests,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms,
		    MAX(latency_ms) AS max_latency_ms,
		    COUNTIF(outcome = "timeout") AS timeout_count,
		    COUNTIF(outcome = "http_4xx") AS http_4xx_count,
		    COUNTIF(outcome = "http_5xx") AS http_5xx_count,
		    COUNTIF(NOT success AND outcome NOT IN ("timeout", "http_4xx", "http_5xx")) AS other_error_count,
		    CURRENT_TIMESTAMP() AS updated_at
		  FROM requests
		  GROUP BY source

		  UNION ALL

		  SELECT
		    @service_date AS service_date,
		    source,
		    FALSE AS is_total,
		    TIMESTAMP_TRUNC(observed_at, HOUR, "America/Los_Angeles") AS hour_start,
		    COUNT(*) AS requests,
		    COUNTIF(success) AS successful_requests,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_latency_ms,
		    APPROX_QUANTILES(latency_ms, 100)[OFFSET(99)] AS p99_latency_ms,
		    MAX(latency_ms) AS max_latency_ms,
		    COUNTIF(outcome = "timeout") AS timeout_count,
		    COUNTIF(outcome = "http_4xx") AS http_4xx_count,
		    COUNTIF(outcome = "http_5xx") AS http_5xx_count,
		    COUNTIF(NOT success AND outcome NOT IN ("timeout", "http_4xx", "http_5xx")) AS other_error_count,
		    CURRENT_TIMESTAMP() AS updated_at
		  FROM requests
		  GROUP BY source, hour_start
		) AS source
		ON target.service_date = source.service_date
		 AND target.source = source.source
		 AND target.is_total = source.is_total
		 AND (target.hour_start = source.hour_start OR (target.hour_start IS NULL AND source.hour_start IS NULL))
		WHEN MATCHED THEN UPDATE SET
		  requests = source.requests,
		  successful_requests = source.successful_requests,
		  p50_latency_ms = source.p50_latency_ms,
		  p95_latency_ms = source.p95_latency_ms,
		  p99_latency_ms = source.p99_latency_ms,
		  max_latency_ms = source.max_latency_ms,
		  timeout_count = source.timeout_count,
		  http_4xx_count = source.http_4xx_count,
		  http_5xx_count = source.http_5xx_count,
		  other_error_count = source.other_error_count,
		  updated_at = source.updated_at
		WHEN NOT MATCHED THEN INSERT (
		  service_date, source, is_total, hour_start, requests, successful_requests,
		  p50_latency_ms, p95_latency_ms, p99_latency_ms, max_latency_ms,
		  timeout_count, http_4xx_count, http_5xx_count, other_error_count, updated_at
		) VALUES (
		  source.service_date, source.source, source.is_total, source.hour_start,
		  source.requests, source.successful_requests, source.p50_latency_ms,
		  source.p95_latency_ms, source.p99_latency_ms, source.max_latency_ms,
		  source.timeout_count, source.http_4xx_count, source.http_5xx_count,
		  source.other_error_count, source.updated_at
		)
	`, projectID, bqDatasetID, apiRequestHourlyBQTable, projectID, bqDatasetID, apiRequestBQTable))
	q.Parameters = []bigquery.QueryParameter{{Name: "service_date", Value: serviceDate}}
	job, err := q.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}

func queryAPIHealthRollupStats(ctx context.Context, serviceDate civil.Date) (*apiHealthStats, error) {
	q := bqClient.Query(fmt.Sprintf(`
		SELECT
		  source, is_total, hour_start, requests, successful_requests,
		  p50_latency_ms, p95_latency_ms, p99_latency_ms, max_latency_ms,
		  timeout_count, http_4xx_count, http_5xx_count, other_error_count
		FROM `+"`%s.%s.%s`"+`
		WHERE service_date = @service_date
		ORDER BY source, is_total DESC, hour_start
	`, projectID, bqDatasetID, apiRequestHourlyBQTable))
	q.Parameters = []bigquery.QueryParameter{{Name: "service_date", Value: serviceDate}}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var rows []apiHealthAggregateRow
	for {
		var row apiHealthAggregateRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return assembleAPIHealthStats(serviceDate, serviceDate, rows), nil
}

func assembleAPIHealthStats(periodStart, periodEnd civil.Date, rows []apiHealthAggregateRow) *apiHealthStats {
	bySource := make(map[string]*apiHealthSourceStats)
	for _, row := range rows {
		source := bySource[row.Source]
		if source == nil {
			source = &apiHealthSourceStats{Source: row.Source}
			bySource[row.Source] = source
		}
		if row.IsTotal {
			source.Requests = row.Requests
			source.SuccessfulRequests = row.SuccessfulRequests
			source.SuccessPct = apiSuccessPct(row.SuccessfulRequests, row.Requests)
			source.P50LatencyMS = nullableAPILatency(row.P50LatencyMS)
			source.P95LatencyMS = nullableAPILatency(row.P95LatencyMS)
			source.P99LatencyMS = nullableAPILatency(row.P99LatencyMS)
			source.MaxLatencyMS = nullableAPILatency(row.MaxLatencyMS)
			source.TimeoutCount = row.TimeoutCount
			source.HTTP4xxCount = row.HTTP4xxCount
			source.HTTP5xxCount = row.HTTP5xxCount
			source.OtherErrorCount = row.OtherErrorCount
			if row.ExpectedPolls.Valid {
				source.ExpectedPolls = row.ExpectedPolls.Int64
			}
			if row.SuccessfulPolls.Valid {
				source.SuccessfulPolls = row.SuccessfulPolls.Int64
			}
			source.PollAvailabilityPct = apiSuccessPct(source.SuccessfulPolls, source.ExpectedPolls)
			if row.LongestOutageMinutes.Valid {
				source.LongestOutageMinutes = row.LongestOutageMinutes.Int64
			}
			if row.LongestOutageStartedAt.Valid {
				startedAt := row.LongestOutageStartedAt.Timestamp
				source.LongestOutageStartedAt = &startedAt
			}
			if row.LongestOutageEndedAt.Valid {
				endedAt := row.LongestOutageEndedAt.Timestamp
				source.LongestOutageEndedAt = &endedAt
			}
			continue
		}
		if !row.HourStart.Valid {
			continue
		}
		source.Buckets = append(source.Buckets, apiHealthBucket{
			StartedAt:          row.HourStart.Timestamp,
			Requests:           row.Requests,
			SuccessfulRequests: row.SuccessfulRequests,
			SuccessPct:         apiSuccessPct(row.SuccessfulRequests, row.Requests),
			P50LatencyMS:       nullableAPILatency(row.P50LatencyMS),
			P95LatencyMS:       nullableAPILatency(row.P95LatencyMS),
			P99LatencyMS:       nullableAPILatency(row.P99LatencyMS),
			MaxLatencyMS:       nullableAPILatency(row.MaxLatencyMS),
			TimeoutCount:       row.TimeoutCount,
			HTTP4xxCount:       row.HTTP4xxCount,
			HTTP5xxCount:       row.HTTP5xxCount,
			OtherErrorCount:    row.OtherErrorCount,
		})
	}

	if len(bySource) == 0 {
		return nil
	}
	for _, source := range bySource {
		sort.Slice(source.Buckets, func(i, j int) bool {
			return source.Buckets[i].StartedAt.Before(source.Buckets[j].StartedAt)
		})
	}

	ordered := make([]apiHealthSourceStats, 0, len(bySource))
	for _, name := range []string{apiSourceVehicleLocations, apiSourceRidership} {
		if source := bySource[name]; source != nil {
			ordered = append(ordered, *source)
			delete(bySource, name)
		}
	}
	otherNames := make([]string, 0, len(bySource))
	for name := range bySource {
		otherNames = append(otherNames, name)
	}
	sort.Strings(otherNames)
	for _, name := range otherNames {
		ordered = append(ordered, *bySource[name])
	}

	return &apiHealthStats{
		PeriodStart:       periodStart.String(),
		PeriodEnd:         periodEnd.String(),
		BucketGranularity: "hour",
		Sources:           ordered,
	}
}

func apiSuccessPct(successful, requests int64) float64 {
	if requests == 0 {
		return 0
	}
	return roundTo(100*float64(successful)/float64(requests), 2)
}

func nullableAPILatency(latency bigquery.NullFloat64) float64 {
	if !latency.Valid {
		return 0
	}
	return round1(latency.Float64)
}
