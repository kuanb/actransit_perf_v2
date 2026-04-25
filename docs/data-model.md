# Data model: vehicle performance tracking

Status: **planned**, not yet implemented. See `architecture.md` for rationale.

Three structured artifacts:

1. `gcs://transit-203605-actransit-cache/gtfs/processed/route_<route_id>.json` — daily-rebuilt static GTFS, indexed for fast lookup
2. `gcs://transit-203605-actransit-cache/state.json` — short-lived in-flight trip state, mutated each minute
3. BigQuery dataset `actransit` — two tables holding completed-trip analytics

## 1. `gtfs/processed/route_<route_id>.json`

One JSON object per route, written by `/refresh-gtfs` only when the source
GTFS hash changes. Joins `trips.txt`, `stop_times.txt`, `stops.txt`, and
`shapes.txt` so the tracker can do a single key lookup at runtime.

```jsonc
{
  "route_id": "1",
  "feed_hash": "0978288308b5...9393",        // for cache-invalidation cross-checks
  "generated_at": "2026-04-25T05:00:00Z",

  "shapes": {                                 // keyed by shape_id
    "shp-001": [
      [37.8044, -122.2712, 0.0],              // [lat, lon, cumulative_distance_meters]
      [37.8046, -122.2715, 32.4],
      // ... thousands of points per shape
    ]
  },

  "stops": {                                  // keyed by stop_id, only stops on this route
    "53704": {
      "stop_id": "53704",
      "stop_name": "International Bl & 14th St",
      "lat": 37.7790,
      "lon": -122.2412
    }
  },

  "trips": {                                  // keyed by trip_id
    "12345-1234": {
      "trip_id": "12345-1234",
      "shape_id": "shp-001",
      "service_id": "WKDY",
      "direction_id": 0,
      "stop_times": [                         // ordered by stop_sequence
        {
          "stop_sequence": 1,
          "stop_id": "53704",
          "arrival_time": "08:30:00",         // GTFS-static format; can exceed 24h
          "departure_time": "08:30:00",
          "dist_along_route_m": 0.0           // precomputed: stop projected onto shape
        },
        // ... ~30 stops per trip
      ]
    }
  }
}
```

**Why precompute `dist_along_route_m` on each stop?** It's a deterministic
function of `(stops[stop_id].lat,lon)` projected onto `shapes[shape_id]`. Doing
it once daily saves the tracker from recomputing it on every probe. Stop
arrival detection becomes a simple "did the probe's distance-along-route
cross this value?" comparison.

Per-file size estimate: 100–500 KB depending on route complexity (shape point
count dominates).

## 2. `state.json`

A single file at `gcs://transit-203605-actransit-cache/state.json` holding
all in-flight trips. Read once + written once per `/track-performance`
invocation. Atomic via `If-Generation-Match`.

```jsonc
{
  "schema_version": 1,
  "updated_at": "2026-04-25T15:23:00Z",
  "in_flight": [
    {
      "vehicle_id": "5001",
      "route_id": "1",
      "trip_id": "12345-1234",
      "service_date": "2026-04-25",           // PT local date when trip started
      "shape_id": "shp-001",
      "first_seen_ts": "2026-04-25T15:01:18Z",
      "last_seen_ts":  "2026-04-25T15:23:00Z",
      "probes": [                             // bounded; old probes shed once stop seq passes them
        {
          "ts": "2026-04-25T15:23:00Z",
          "lat": 37.8044,
          "lon": -122.2712,
          "bearing_deg": 285.0,
          "reported_speed_mps": 8.4,
          "dist_along_route_m": 1542.7,
          "nearest_stop_seq": 5
        }
        // ...
      ],
      "stop_arrivals": {                      // keyed by stop_sequence (string for JSON portability)
        "1": "2026-04-25T15:01:18Z",
        "2": "2026-04-25T15:03:05Z",
        "3": "2026-04-25T15:04:42Z"
        // populated as each stop is crossed
      }
    }
    // ... one entry per active trip
  ]
}
```

**Steady-state size**: ~200 in-flight trips × ~250 bytes each = ~50 KB.

**Probe retention**: keep only probes from the last 5 minutes — after that,
they've already been used to detect stop arrivals and don't add value to
in-flight processing. Reduces state size and write cost.

**Trip lifecycle in state**:

```
not_in_state
    │
    │ probe arrives with new (vehicle_id, trip_id) tuple
    ▼
in_state, accumulating probes + detecting stop arrivals
    │
    ├── reaches last stop → finalize, write to BigQuery, remove from state
    │
    └── no probe seen for ≥ 20 min → finalize what we have,
                                      mark stale=true, write to BigQuery,
                                      remove from state
```

## 3. BigQuery: dataset `actransit`

Project: `transit-203605`. Region: `us-west1` (same as bucket and Cloud Run,
keeps egress free).

### `actransit.trip_observations`

One row per (trip_id, stop_sequence). The "did the bus arrive on time?"
answer.

```sql
CREATE TABLE actransit.trip_observations (
  service_date          DATE        NOT NULL,
  route_id              STRING      NOT NULL,
  trip_id               STRING      NOT NULL,
  vehicle_id            STRING      NOT NULL,
  stop_sequence         INT64       NOT NULL,
  stop_id               STRING      NOT NULL,
  scheduled_arrival     TIMESTAMP,                    -- nullable: stop-times missing happens
  actual_arrival        TIMESTAMP,                    -- nullable if trip went stale before reaching stop
  delay_seconds         INT64,                        -- actual − scheduled
  leg_distance_m        FLOAT64,                      -- from previous stop_sequence's projected position
  leg_duration_s        FLOAT64,                      -- from previous stop's actual_arrival
  leg_avg_speed_mps     FLOAT64,                      -- leg_distance_m / leg_duration_s
  is_stale              BOOL        NOT NULL,         -- true if trip finalized via timeout, not completion
  ingested_at           TIMESTAMP   NOT NULL          -- when this row was written
)
PARTITION BY service_date
CLUSTER BY route_id, trip_id;
```

**Why partition by `service_date`?** Most analytical queries scope to a date
range ("how was on-time performance last week?"). Partitioning prunes scan
cost dramatically.

**Why cluster by `(route_id, trip_id)`?** The other common filter shape is
"all observations for route 1" or "all observations for trip X." Clustering
keeps those rows physically co-located within a partition.

**Schedule arrival timezone**: stored as `TIMESTAMP` (UTC), converted from
GTFS-static's `HH:MM:SS` × `service_date` × agency timezone (`America/Los_Angeles`).
Handles the "GTFS times can exceed 24:00" quirk for trips that cross midnight.

### `actransit.trip_probes`

One row per raw probe observation, projected onto the route. The audit log.

```sql
CREATE TABLE actransit.trip_probes (
  service_date          DATE        NOT NULL,
  route_id              STRING      NOT NULL,
  trip_id               STRING      NOT NULL,
  vehicle_id            STRING      NOT NULL,
  observed_at           TIMESTAMP   NOT NULL,
  lat                   FLOAT64     NOT NULL,
  lon                   FLOAT64     NOT NULL,
  bearing_deg           FLOAT64,
  reported_speed_mps    FLOAT64,
  dist_along_route_m    FLOAT64     NOT NULL,
  nearest_stop_seq      INT64,
  ingested_at           TIMESTAMP   NOT NULL
)
PARTITION BY service_date
CLUSTER BY route_id, trip_id;
```

**Volume estimate**: ~200 active vehicles × 60 probes/hour × 18 hours = ~216k
rows/day = ~78M rows/year. At ~80 bytes/row uncompressed, ~6 GB/year. Inside
free-tier storage indefinitely.

**Use cases**:
- Replay any historical trip frame-by-frame
- Spot-check stop-arrival detection accuracy
- Train/validate ETA models later

### Common query examples

On-time performance for a route last week:

```sql
SELECT
  AVG(IF(delay_seconds <= 60, 1, 0)) AS on_time_rate,
  APPROX_QUANTILES(delay_seconds, 100)[OFFSET(95)] AS p95_delay_seconds
FROM actransit.trip_observations
WHERE service_date BETWEEN DATE_SUB(CURRENT_DATE("America/Los_Angeles"), INTERVAL 7 DAY)
                       AND CURRENT_DATE("America/Los_Angeles")
  AND route_id = '1'
  AND actual_arrival IS NOT NULL;
```

Replay every probe for a specific trip:

```sql
SELECT observed_at, lat, lon, dist_along_route_m, reported_speed_mps
FROM actransit.trip_probes
WHERE service_date = '2026-04-25'
  AND trip_id = '12345-1234'
ORDER BY observed_at;
```

Avg leg speed by route, last 7 days:

```sql
SELECT
  route_id,
  AVG(leg_avg_speed_mps) AS avg_mps,
  AVG(leg_avg_speed_mps) * 2.2369 AS avg_mph
FROM actransit.trip_observations
WHERE service_date BETWEEN DATE_SUB(CURRENT_DATE("America/Los_Angeles"), INTERVAL 7 DAY)
                       AND CURRENT_DATE("America/Los_Angeles")
  AND leg_avg_speed_mps IS NOT NULL
GROUP BY route_id
ORDER BY avg_mph DESC;
```

## Schema evolution

Both tables include `ingested_at` so we can reprocess if logic changes —
filter on `ingested_at` to find rows from a specific tracker version. If we
ever need a hard schema break, the partition layout makes "drop one day,
reingest" cheap.
