WITH ranked AS (
  SELECT
    service_date,
    observed_at,
    vehicle_id,
    route_id,
    trip_id,
    current_passenger_count,
    vehicle_capacity,
    estimated_occupancy_percentage,
    estimated_occupancy_status,
    ROW_NUMBER() OVER (
      PARTITION BY service_date, observed_at, vehicle_id
      ORDER BY ingested_at DESC
    ) AS row_number
  FROM `transit-203605.actransit.ridership_observations`
  WHERE service_date BETWEEN '2026-10-01' AND '2026-10-31'
), observations AS (
  SELECT
    service_date,
    observed_at,
    vehicle_id,
    route_id,
    trip_id,
    current_passenger_count,
    CASE
      WHEN current_passenger_count IS NOT NULL THEN current_passenger_count
      WHEN estimated_occupancy_percentage IS NOT NULL THEN
        ROUND(vehicle_capacity * estimated_occupancy_percentage / 100)
      WHEN LOWER(estimated_occupancy_status) = 'not crowded' THEN ROUND(vehicle_capacity * 0.30)
      WHEN LOWER(estimated_occupancy_status) = 'some crowding' THEN ROUND(vehicle_capacity * 0.60)
      WHEN LOWER(estimated_occupancy_status) = 'crowded' THEN ROUND(vehicle_capacity * 0.90)
    END AS estimated_load,
    CASE
      WHEN current_passenger_count IS NOT NULL THEN 'direct_count'
      WHEN estimated_occupancy_percentage IS NOT NULL THEN 'occupancy_percentage'
      WHEN estimated_occupancy_status IS NOT NULL AND vehicle_capacity IS NOT NULL THEN 'status_band'
    END AS estimate_source,
    CASE EXTRACT(DAYOFWEEK FROM service_date)
      WHEN 1 THEN 'sunday'
      WHEN 7 THEN 'saturday'
      ELSE 'weekday'
    END AS day_type
  FROM ranked
  WHERE row_number = 1 AND route_id IS NOT NULL
), snapshot_summary AS (
  SELECT
    route_id,
    day_type,
    COUNTIF(estimated_load IS NOT NULL) AS snapshot_n,
    AVG(estimated_load) AS avg_estimated_load,
    COUNTIF(estimate_source = 'direct_count') AS direct_count_n,
    COUNTIF(estimate_source = 'occupancy_percentage') AS occupancy_percentage_n,
    COUNTIF(estimate_source = 'status_band') AS status_estimate_n
  FROM observations
  GROUP BY route_id, day_type
), trip_maxima AS (
  SELECT
    route_id,
    day_type,
    service_date,
    trip_id,
    MAX(estimated_load) AS max_estimated_load
  FROM observations
  WHERE trip_id IS NOT NULL AND estimated_load IS NOT NULL
  GROUP BY route_id, day_type, service_date, trip_id
), trip_summary AS (
  SELECT
    route_id,
    day_type,
    COUNT(*) AS trip_n,
    AVG(max_estimated_load) AS avg_trip_max_estimated_load
  FROM trip_maxima
  GROUP BY route_id, day_type
)
SELECT
  snapshots.route_id,
  snapshots.day_type,
  snapshots.snapshot_n,
  snapshots.avg_estimated_load,
  trips.trip_n,
  trips.avg_trip_max_estimated_load,
  snapshots.direct_count_n,
  snapshots.occupancy_percentage_n,
  snapshots.status_estimate_n
FROM snapshot_summary AS snapshots
LEFT JOIN trip_summary AS trips USING (route_id, day_type)
ORDER BY route_id, day_type;
