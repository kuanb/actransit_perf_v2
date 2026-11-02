WITH ranked AS (
  SELECT
    service_date,
    observed_at,
    vehicle_id,
    route_id,
    ROW_NUMBER() OVER (
      PARTITION BY service_date, observed_at, vehicle_id
      ORDER BY ingested_at DESC
    ) AS row_number
  FROM `transit-203605.actransit.ridership_observations`
  WHERE service_date BETWEEN '2026-10-01' AND '2026-10-31'
)
SELECT
  route_id,
  vehicle_id,
  COUNT(*) AS snapshot_n
FROM ranked
WHERE row_number = 1 AND route_id IS NOT NULL
GROUP BY route_id, vehicle_id
ORDER BY route_id, vehicle_id;
