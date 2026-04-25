# Architecture: vehicle performance tracking

Status: **planned**, not yet implemented. See `README.md` for the services that
are deployed today (`/scrape`, `/refresh-stops`, `/refresh-gtfs`).

## Goal

Track each scheduled trip a vehicle runs from origin to destination, compute
per-stop scheduled-vs-actual arrival times and per-leg average speed, and
write the results to BigQuery for analytical queries. Surface a real-time
"vehicles being watched" count on the existing Cloud Monitoring dashboard.

## Components

```
                  ┌────────────────────────────────────────────────────┐
                  │                  Cloud Run service                 │
                  │  (existing: /scrape /refresh-stops /refresh-gtfs)  │
                  │                                                    │
[every 1 min]──▶  │  /scrape           writes latest.json (existing)   │
[every 1 min]──▶  │  /track-performance (new)                          │
                  │     1. read latest.json + state.json from GCS      │
                  │     2. load static GTFS from in-mem cache          │
                  │        (loaded at cold start, refreshed by gen #)  │
                  │     3. for each vehicle: project probe → compute   │
                  │        distance-along-route, detect stop crossings │
                  │     4. update in-flight state                      │
                  │     5. for completed/stale trips: write to BQ      │
                  │     6. emit custom metrics                         │
                  │     7. write state.json back                       │
                  └────────────┬───────────────────────────────────────┘
                               │
                  ┌────────────┴───────────────────────────────────────┐
                  │                                                    │
                  ▼                                                    ▼
         ┌────────────────┐                                 ┌─────────────────────┐
         │ GCS state.json │ in-flight trips (~50 KB JSON)   │ BigQuery: completed │
         │ ←── ephemeral  │                                 │ trip rows           │
         └────────────────┘                                 │ partitioned by date │
                                                            │ clustered by route  │
         ┌────────────────────────────┐                     └─────────────────────┘
         │ GCS gtfs/processed/        │
         │   route_<route_id>.json    │  written daily by /refresh-gtfs
         │   ~200 KB × ~100 routes    │
         └────────────────────────────┘
```

## Decisions

Each decision below records the chosen option and the alternatives considered,
so future reviewers (including future-you) can understand *why*, not just
*what*.

### Compute: extend the existing Cloud Run service

A new endpoint `/track-performance` lives in the same binary as
`/scrape`/`/refresh-stops`/`/refresh-gtfs`. Reuses image, service account,
secret IAM, GCS bucket access. One image to maintain.

The existing scraper SA already has `roles/storage.objectUser` on the cache
bucket and `roles/secretmanager.secretAccessor` on the API token secret. We'll
add one new permission later: `roles/bigquery.dataEditor` on the `actransit`
dataset.

### Trigger: separate Cloud Scheduler job, every minute

Three options were considered:

1. **Inline in `/scrape`** — adds work to the critical scrape path; one bug
   breaks both. Worse failure isolation.
2. **`/scrape` HTTP-POSTs to `/track-performance` at end of handler** — couples
   them in code but keeps the trigger simple.
3. **Separate Scheduler job hitting `/track-performance` every minute** —
   chosen.

(3) wins on failure isolation: if the tracker crashes, scrapes keep running
and we just lose minutes of analytics until it recovers. Logs from the two
endpoints are also cleanly separated for debugging. Cloud Scheduler is in the
free tier through 3 jobs (we have `scrape`, `refresh-stops`, `refresh-gtfs`,
and now `track-performance` — first paid job at $0.10/month).

Reading the freshest `latest.json` is consistent regardless of trigger timing
because the tracker just reads whatever GCS returns when called; small race
windows don't matter (the next minute fixes it).

### State: GCS `state.json`, single object

The tracker is stateful — it must remember "vehicle 5001 has been on trip
12345 since 08:30, here are its probe positions and detected stop arrivals."
That state is short-lived (one trip ≈ 30–90 min) but mutable each minute.

Options considered:

| Option | Verdict |
|---|---|
| **Firestore** | Rejected. ~200 active vehicles × 60 reads/min × 18 active hours ≈ 216k reads/day vs. 50k free tier — would exceed quota daily. |
| **Always-on Cloud Run** (`min_instances=1` with in-memory state) | Rejected. Memory billing for 256 Mi held-warm = ~$25/mo. Out of budget. |
| **GCS `state.json`** | **Chosen.** ~$0.22/month for 60 writes/hour. |
| **Cloud SQL** | Rejected. db-f1-micro is $7/mo and the schema is overkill for a single small JSON blob. |

Race protection: GCS supports `If-Generation-Match` precondition writes for
atomic "write only if state hasn't changed since I read it." With
`max_instance_count = 2` on Cloud Run, two concurrent `/track-performance`
invocations are theoretically possible. The precondition check makes this
safe — a losing writer retries by re-reading state and recomputing.

Steady-state size: ~50 KB (50–200 in-flight trips × small probe history each).

### Warehouse: BigQuery

Native GCP analytical store. Free tier: 10 GiB storage + 1 TiB query/month
+ 2 GB streaming inserts/month. At our data scale (single-digit GB/year),
storage and ingest are free; query cost is bounded by partition-pruning.

Two tables under dataset `actransit`:

- `trip_observations` — one row per (trip_id, stop_seq); the rolled-up "did
  the bus arrive on time?" answer.
- `trip_probes` — one row per probe observation; the audit log enabling full
  replay of a trip.

Both partitioned by `service_date` (DATE), clustered by `route_id`. See
`data-model.md` for full schemas.

Writes: BigQuery streaming inserts via the Storage Write API (more efficient
than legacy `tabledata.insertAll`).

### GTFS static: per-route processed JSON in GCS

The full GTFS zip in `gtfs/current.zip` is ~14 MB compressed (~50 MB
uncompressed across all tables). Parsing it on every minutely invocation
would burn CPU. Solution: daily preprocessing into structured JSON, plus
in-memory caching at Cloud Run cold start.

Daily `/refresh-gtfs` job adds preprocessing:

1. Download the zip (existing).
2. Compute hash, dedup, write archive + current.zip (existing).
3. **New**: when the hash changed, unzip in memory, join `trips.txt` ×
   `stop_times.txt` × `stops.txt` × `shapes.txt`, and write one JSON file
   per `route_id` to `gs://transit-203605-actransit-cache/gtfs/processed/route_<id>.json`.

Per-route splitting was chosen over a single big file for: (a) cleaner GCS
browsing, (b) future-proofing if we add per-route services, (c) simpler
incremental reprocessing if we ever need it. Cost difference vs. one big
file is essentially zero.

### Cache invalidation

The Cloud Run instance loads processed GTFS into memory at cold start.
GTFS releases land at most weekly. Strategy:

- On cold start, load all `gtfs/processed/route_*.json` and remember the
  generation number of `gtfs/current.zip`.
- Every ~100 `/track-performance` invocations (≈ every 100 min), read the
  generation of `gtfs/current.zip`. If newer, reload processed files.

Simple and good enough — staleness window is at most 100 min after a new
GTFS landing, which is well under the cadence at which AC Transit updates
schedules.

### Stale trip timeout: 20 minutes

If we haven't seen a vehicle's probe for 20 minutes, finalize whatever we
have and emit it to BigQuery. AC Transit's GPS occasionally drops; 20 min
balances "give the bus a chance to come back" with "don't pin in-flight
state forever."

## Cost projection

| Line item | Estimate |
|---|---|
| BigQuery storage (probes + observations) | ~$0.05/mo at ~1 GB |
| BigQuery streaming inserts | $0/mo (free tier 2 GB/mo > our 6.5 GB/yr) |
| BigQuery query (manual analytical queries) | $0 (free tier 1 TiB/mo) |
| GCS `state.json` reads + writes (60/min) | ~$0.22/mo |
| GCS `gtfs/processed/*` (1 daily write batch + cold-start reads) | ~$0.01/mo |
| Cloud Run invocations (2× existing rate) | $0 (free tier) |
| Cloud Scheduler (4th job) | $0.10/mo (first paid job) |
| Cloud Monitoring custom metrics | $0/mo (first 6 free) |

Total marginal cost: **~$0.40/month** on top of the existing ~$1/month.

## Phased rollout

| # | Adds | Why this size |
|---|---|---|
| 1 | Augment `/refresh-gtfs` to write `gtfs/processed/route_*.json` per route. No new endpoint yet. | Foundation; testable in isolation by inspecting JSON output. |
| 2 | New `/track-performance` endpoint with **trip lifecycle state machine only** — detect trip start/end from realtime feed, write `state.json`, emit `vehicles_in_flight` metric. No projection yet. | Smallest meaningful slice; "vehicles being watched" counter on the dashboard. |
| 3 | **Route projection + stop-arrival detection**. Output stays in state.json. Spot-check by comparing scheduled vs. detected arrivals manually. | Hardest math; isolating it lets us iterate without DB schema concerns. |
| 4 | **BigQuery dataset + tables + write path**. Completed/stale trips emit rows to `trip_observations` + `trip_probes`. | One-shot wiring; tables are partitioned/clustered correctly from day one. |
| 5 | **Stale-trip detector + dashboard tiles**. Add tiles for completed trips/day, avg delay, and a 5th tile for `vehicles_in_flight` on the existing dashboard. | Polish + ops resilience. |
