WITH obs AS (
  SELECT *
  FROM `transit-203605.actransit.trip_observations`
  WHERE service_date BETWEEN "2026-05-17" AND "2026-08-18"
    AND service_date NOT IN ("2026-06-14", "2026-08-09")
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY service_date, trip_id, stop_sequence
    ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
  ) = 1
),
per_trip AS (
  SELECT
    service_date,
    DATE_TRUNC(service_date, WEEK(SUNDAY)) AS week_start,
    route_id,
    trip_id,
    CASE
      WHEN service_date BETWEEN "2026-05-17" AND "2026-06-13" THEN "Baseline"
      WHEN service_date BETWEEN "2026-06-14" AND "2026-08-08" THEN "Summer"
      WHEN service_date BETWEEN "2026-08-09" AND "2026-08-18" THEN "Post-shift"
    END AS period,
    CASE
      WHEN route_id IN ("701", "702", "703") THEN "Early Bird"
      WHEN route_id IN ("800", "801", "802", "805", "840", "851") THEN "All Nighter"
      WHEN SAFE_CAST(route_id AS INT64) BETWEEN 600 AND 699 THEN "School"
      WHEN route_id IN ("E", "F", "FS", "G", "J", "L", "NL", "NX", "NX3", "O", "P", "U", "V", "W") THEN "Transbay"
      ELSE "Local / other"
    END AS route_group,
    MAX(stop_sequence) AS final_seq,
    MAX(IF(actual_arrival IS NOT NULL, stop_sequence, NULL)) AS last_observed_seq
  FROM obs
  GROUP BY service_date, week_start, route_id, trip_id, period, route_group
),
ran AS (
  SELECT *, 100 * SAFE_DIVIDE(last_observed_seq, final_seq) AS completion_pct
  FROM per_trip
  WHERE last_observed_seq IS NOT NULL AND final_seq > 0
),
period_result AS (
  SELECT
    "period" AS grain,
    CAST(NULL AS DATE) AS week_start,
    period,
    route_group,
    COUNT(*) AS ran_trips,
    COUNTIF(last_observed_seq >= final_seq) AS reached_final_stop,
    COUNTIF(last_observed_seq < final_seq) AS incomplete_trips,
    COUNTIF(completion_pct < 80) AS severely_truncated_trips,
    APPROX_QUANTILES(completion_pct, 100)[OFFSET(5)] AS p05_completion_pct,
    APPROX_QUANTILES(completion_pct, 100)[OFFSET(50)] AS p50_completion_pct
  FROM ran
  GROUP BY period, route_group
),
week_result AS (
  SELECT
    "week" AS grain,
    week_start,
    CAST(NULL AS STRING) AS period,
    route_group,
    COUNT(*) AS ran_trips,
    COUNTIF(last_observed_seq >= final_seq) AS reached_final_stop,
    COUNTIF(last_observed_seq < final_seq) AS incomplete_trips,
    COUNTIF(completion_pct < 80) AS severely_truncated_trips,
    APPROX_QUANTILES(completion_pct, 100)[OFFSET(5)] AS p05_completion_pct,
    APPROX_QUANTILES(completion_pct, 100)[OFFSET(50)] AS p50_completion_pct
  FROM ran
  GROUP BY week_start, route_group
)
SELECT * FROM period_result
UNION ALL
SELECT * FROM week_result
ORDER BY grain, week_start, period, route_group;
