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
arrivals AS (
  SELECT
    service_date,
    DATE_TRUNC(service_date, WEEK(SUNDAY)) AS week_start,
    route_id,
    stop_id,
    actual_arrival,
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
    END AS route_group
  FROM obs
  WHERE actual_arrival IS NOT NULL
),
with_previous AS (
  SELECT
    *,
    TIMESTAMP_DIFF(
      actual_arrival,
      LAG(actual_arrival) OVER (PARTITION BY service_date, route_id, stop_id ORDER BY actual_arrival),
      SECOND
    ) AS headway_seconds
  FROM arrivals
),
valid AS (
  SELECT *, 60 * CAST(ROUND(headway_seconds / 60.0) AS INT64) AS headway_bin_seconds
  FROM with_previous
  WHERE headway_seconds BETWEEN 60 AND 7200
),
period_group AS (
  SELECT "period_group" AS grain, period, CAST(NULL AS DATE) AS week_start, route_group, CAST(NULL AS STRING) AS route_id, headway_bin_seconds, COUNT(*) AS n
  FROM valid
  GROUP BY period, route_group, headway_bin_seconds
),
week_group AS (
  SELECT "week_group" AS grain, CAST(NULL AS STRING) AS period, week_start, route_group, CAST(NULL AS STRING) AS route_id, headway_bin_seconds, COUNT(*) AS n
  FROM valid
  GROUP BY week_start, route_group, headway_bin_seconds
),
period_route AS (
  SELECT "period_route" AS grain, period, CAST(NULL AS DATE) AS week_start, route_group, route_id, headway_bin_seconds, COUNT(*) AS n
  FROM valid
  GROUP BY period, route_group, route_id, headway_bin_seconds
)
SELECT * FROM period_group
UNION ALL
SELECT * FROM week_group
UNION ALL
SELECT * FROM period_route
ORDER BY grain, period, week_start, route_group, route_id, headway_bin_seconds;
