WITH raw AS (
  SELECT service_date, COUNT(*) AS raw_rows
  FROM `transit-203605.actransit.trip_observations`
  WHERE service_date BETWEEN "2026-04-18" AND "2026-08-18"
  GROUP BY service_date
),
obs AS (
  SELECT *
  FROM `transit-203605.actransit.trip_observations`
  WHERE service_date BETWEEN "2026-04-18" AND "2026-08-18"
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY service_date, trip_id, stop_sequence
    ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
  ) = 1
),
deduped AS (
  SELECT
    service_date,
    COUNT(*) AS deduped_rows,
    COUNT(DISTINCT route_id) AS routes,
    COUNT(DISTINCT IF(actual_arrival IS NOT NULL, trip_id, NULL)) AS trips_observed,
    COUNT(DISTINCT IF(actual_arrival IS NOT NULL, vehicle_id, NULL)) AS vehicles_observed,
    COUNTIF(actual_arrival IS NOT NULL) AS arrivals,
    COUNTIF(actual_arrival IS NOT NULL AND is_stale = FALSE) AS nonstale_arrivals
  FROM obs
  GROUP BY service_date
)
SELECT
  d.*,
  r.raw_rows,
  r.raw_rows - d.deduped_rows AS duplicate_rows,
  ROUND(100 * SAFE_DIVIDE(r.raw_rows - d.deduped_rows, r.raw_rows), 1) AS duplicate_pct
FROM deduped d
JOIN raw r USING (service_date)
ORDER BY service_date;
