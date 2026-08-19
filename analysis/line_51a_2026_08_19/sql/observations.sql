WITH ranked AS (
  SELECT
    FORMAT_DATE("%F", service_date) AS service_date,
    trip_id,
    vehicle_id,
    stop_sequence,
    stop_id,
    FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%E6SZ", actual_arrival) AS actual_arrival,
    delay_seconds,
    is_stale,
    COUNT(*) OVER (
      PARTITION BY service_date, trip_id, stop_sequence
    ) AS finalization_copies,
    ROW_NUMBER() OVER (
      PARTITION BY service_date, trip_id, stop_sequence
      ORDER BY IF(actual_arrival IS NULL, 1, 0), ingested_at DESC
    ) AS row_rank
  FROM `transit-203605.actransit.trip_observations`
  WHERE service_date BETWEEN "2026-06-01" AND "2026-07-31"
    AND service_date != "2026-06-14"
    AND route_id = "51A"
)
SELECT * EXCEPT (row_rank)
FROM ranked
WHERE row_rank = 1
ORDER BY service_date, trip_id, stop_sequence;
