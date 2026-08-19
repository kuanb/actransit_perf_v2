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
classified AS (
  SELECT
    *,
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
),
aggregated AS (
  SELECT
    IF(GROUPING(route_id) = 1, "group", "route") AS grain,
    period,
    route_group,
    route_id,
    COUNT(DISTINCT service_date) AS service_days,
    COUNT(*) AS scheduled_stop_rows,
    COUNTIF(actual_arrival IS NOT NULL AND delay_seconds BETWEEN -60 AND 420) AS delivered_stop_rows,
    COUNT(DISTINCT IF(actual_arrival IS NOT NULL, CONCAT(CAST(service_date AS STRING), "|", trip_id), NULL)) AS observed_trips,
    COUNTIF(actual_arrival IS NOT NULL) AS arrivals,
    COUNTIF(actual_arrival IS NOT NULL AND delay_seconds BETWEEN 0 AND 180) AS on_time_arrivals,
    COUNTIF(actual_arrival IS NOT NULL AND delay_seconds < 0) AS early_arrivals,
    COUNTIF(actual_arrival IS NOT NULL AND delay_seconds > 180) AS late_arrivals,
    COUNTIF(actual_arrival IS NOT NULL AND delay_seconds > 420) AS very_late_arrivals,
    APPROX_QUANTILES(IF(actual_arrival IS NOT NULL, delay_seconds, NULL), 100)[OFFSET(5)] AS p05_delay_seconds,
    APPROX_QUANTILES(IF(actual_arrival IS NOT NULL, delay_seconds, NULL), 100)[OFFSET(50)] AS p50_delay_seconds,
    APPROX_QUANTILES(IF(actual_arrival IS NOT NULL, delay_seconds, NULL), 100)[OFFSET(95)] AS p95_delay_seconds
  FROM classified
  GROUP BY GROUPING SETS ((period, route_group), (period, route_group, route_id))
)
SELECT *
FROM aggregated
ORDER BY grain, period, route_group, route_id;
