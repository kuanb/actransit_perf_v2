# AGENTS.md

Source of truth for AI coding agents working in this repo. Other agent
configs (`CLAUDE.md`, `.cursor/rules/*.mdc`) point here — keep this file
authoritative, don't fork conventions into them.

User-facing project docs are in `README.md` and `docs/`. Read those for
*what* the system is. This file covers *how to work in it*.

## Project at a glance

- **Purpose:** minutely AC Transit GTFS-Realtime scraper + per-trip
  performance tracker on GCP. Personal pet project, ~$1–2/month budget.
- **Migration:** ported from val.town. Owner is learning Go and GCP,
  so explanations that connect to first principles or GCP idioms are
  welcome over jargon.
- **GCP project ID:** `transit-203605` (us-west1).
- **Public cache bucket:** `transit-203605-actransit-cache` (world-readable).

## Repo layout

```
cmd/scraper/        Single Go binary; one HTTP handler per Cloud Scheduler job
  main.go             Routes, GCS read/write, /scrape, /refresh-stops, /refresh-gtfs,
                      /generate-daily-stats, /backfill-day handler wiring
  track.go            /track-performance handler + state machine + metric emit +
                      arrival detection + trailing-stop tolerance fallback
  bq.go               BigQuery row construction (tripToRows) — applies the
                      trailing-stop tolerance at finalization for both live and backfill
  backfill.go         /backfill-day: replays gs://ac-transit/maptime/{ts}.csv into
                      BQ via load jobs (partition decorator + WriteTruncate)
  stats.go            /generate-daily-stats: GTFS-static + BQ aggregations →
                      stats/<date>.json (system summary, schedule compliance,
                      headway distortion histogram, per-route table)
  gtfs_static.go      Static GTFS download + per-route JSON processing
  gtfs_cache.go       In-process cache of processed static GTFS, with periodic
                      generation-aware refresh check against gtfs/current.zip
  geo.go              Shape projection (haversine + lat/lon → dist-along-route)
infra/              Terraform (state in gs://transit-203605-tfstate)
  scraper.tf          Cloud Run service (2 GiB / 1 vCPU, 1800s timeout)
  schedule.tf         Cloud Scheduler jobs (cron, OIDC invoker)
  bigquery.tf         Datasets + tables (partitioned by service_date, clustered
                      by route_id, trip_id)
  storage.tf          GCS buckets
  alerting.tf         Email alert policies
  dashboard.tf        Cloud Monitoring dashboard JSON
  iam.tf              Service accounts and bindings
docs/               Architecture and data-model design docs
site/               Static frontend (GitHub Pages, auto-deploys on push to main)
Makefile            Authoritative entry points — see README.md "Deploy"
```

## Cron jobs (all run via Cloud Scheduler with OIDC)

| Endpoint                  | Cron               | Time zone | Side effects                                   |
|---------------------------|--------------------|-----------|------------------------------------------------|
| `/scrape`                 | `* * * * *`        | UTC       | Writes `latest.json`, appends to `history.json` |
| `/track-performance`      | `* * * * *`        | UTC       | Reads `latest.json` + `state.json`, writes `state.json`, finalizes trips into BQ, emits two custom metrics |
| `/refresh-stops`          | `0 */6 * * *`      | UTC       | Writes `route_stops.json` |
| `/refresh-gtfs`           | `0 22 * * *`       | PT        | Refreshes `gtfs/current.zip` + per-route processed JSONs |
| `/generate-daily-stats`   | `0 2 * * *`        | PT        | Reads BQ for prior PT day, writes `stats/<date>.json`, `stats/latest.json`, updates `stats/_index.json` |

Manual-only endpoints (no Cloud Scheduler):

- **`/backfill-day?service_date=YYYY-MM-DD[&force=true]`** — replays
  cross-account `gs://ac-transit/maptime/{ts}.csv` snapshots into BQ for
  one past PT day, then re-runs `/generate-daily-stats` for that date.
  Uses BQ load jobs against the partition decorator with `WRITE_TRUNCATE`
  semantics so it's idempotent and safely re-runnable. Refuses today-or-
  future without `force=true`. Use `make backfill DATE=YYYY-MM-DD`.
- **`/refresh-gtfs?force=true`** — bypasses the unchanged-hash skip; lets
  you reprocess the cached zip without waiting for AC Transit to roll a
  new feed. Useful when an earlier refresh half-succeeded.

Two custom Cloud Monitoring metrics emitted from `/track-performance`:
`custom.googleapis.com/actransit/vehicles_in_flight` and
`.../trips_finalized_per_minute`. Both gauge int64, resource type `global`.

## Working conventions

### Code

- Go 1.25, single binary, distroless image. No internal package split yet.
- Default to no comments. Don't write comments that re-state the code or
  reference the current task ("added for X"). Only write a comment when
  *why* is non-obvious (hidden constraint, surprising invariant, workaround
  for a real bug).
- No backwards-compat shims for unmerged code. If a symbol is unused, delete
  it; don't leave `// removed` markers or `_ = unused` lines.
- Don't add error handling, fallbacks, or validation for impossible cases.
  Validate at boundaries (HTTP, GCS, BQ); trust internal call sites.
- Don't add abstractions for hypothetical future requirements. Three similar
  lines beats a premature helper.

### Commits

- **No Claude attribution.** Strip the `Co-Authored-By: Claude ...` trailer
  before committing. Vanilla commit messages only.
- One concept per commit; the message focuses on *why*, not *what*.

### Documentation

- Don't create new `.md` files unless asked. Update existing ones (this file,
  `README.md`, `docs/*`) when conventions change.

## Local + CI commands

```sh
make test                   # go test ./... -race -v   (must pass before commit)
make tf-fmt                 # terraform fmt -recursive
make hooks-install          # install repo's pre-commit hook (vet + test)
make run-local              # local Go server (limited — talks to live GCS)
```

Pre-commit hook runs `go vet` + `go test`; GitHub Actions repeats them on
push. Don't bypass hooks (`--no-verify`) without an explicit request.

## Deploy commands

See README.md "Deploy" for the full reference. The two you'll use:

```sh
make release TAG=vN         # build + push image, terraform apply  (Go changes)
make deploy  TAG=<current>  # terraform apply at known-good tag    (infra-only)
```

For pure Terraform changes, **always pass the current tag** — omitting it
silently downgrades the image to the variable's default `v1`. Find the
deployed tag with `gcloud run services describe actransit-scraper
--region us-west1 --format='value(spec.template.spec.containers[].image)'`.

After deploy, `make smoke` hits the live service end-to-end.

## Diagnostic commands worth keeping handy

```sh
# Recent structured logs (Makefile alias):
make logs

# Filter to a specific event:
gcloud logging read \
  'resource.labels.service_name="actransit-scraper" AND jsonPayload.msg="<msg>"' \
  --limit 20 --freshness=15m \
  --format='value(timestamp,jsonPayload.msg,jsonPayload.err)'

# Read a custom metric directly (point count, not chart aggregation):
PROJECT=transit-203605
START=$(date -u -v-15M +%Y-%m-%dT%H:%M:%SZ)
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  "https://monitoring.googleapis.com/v3/projects/${PROJECT}/timeSeries?\
filter=metric.type%3D%22custom.googleapis.com%2Factransit%2Fvehicles_in_flight%22&\
interval.startTime=${START}&interval.endTime=${END}"

# Inspect public-object cache headers (latest.json staleness checks):
curl -sI https://storage.googleapis.com/transit-203605-actransit-cache/latest.json
```

When the dashboard looks wrong, query the time series API directly before
trusting the chart — chart alignment periods can hide both gaps and bugs.

## Known footguns

- **`/refresh-gtfs` sits within ~10–20% of the Cloud Run memory limit
  during per-route JSON marshaling.** It was OOM-killed silently at 1 GiB
  (peak ~1.2 GiB), wrote only ~76 of 123 route JSONs, returned 503, and
  left the in-process `gtfsCache` permanently incomplete on every container
  restart. Symptom in the data: `/track-performance` produces probe rows
  but no observation rows for the missing routes (`tripToRows` requires a
  cache hit on `route_id` + `trip_id` to synthesize observation rows; on
  miss it silently emits probes only). Memory is now `2 GiB`. If a future
  feed grows substantially, watch refresh-gtfs's peak; bump or stream the
  output to GCS instead of holding all 123 marshaled JSONs in memory.

- **BigQuery's streaming buffer blocks DML for 30–90 minutes.** Streaming
  inserts (`Inserter().Put`) deposit rows into a hot in-memory layer
  queryable immediately but not modifiable by `UPDATE` / `DELETE` /
  `MERGE` until they flush to columnar storage. `/track-performance` (low
  volume, append-only) uses streaming. `/backfill-day` (bulk replace) uses
  load jobs against the partition decorator (`table$YYYYMMDD`) with
  `WriteDisposition: WRITE_TRUNCATE` — load jobs ignore the streaming
  buffer entirely. Don't switch backfill to DML; you'll re-introduce
  `Error 400: would affect rows in the streaming buffer` on every retry
  within the buffer's flush window. `bq rm -t 'table$YYYYMMDD'` does
  **not** clear the streaming buffer either.

- **`tripToRows` applies a 150 m trailing-stop tolerance fallback at
  finalization.** Strictly bracketing arrival detection (`arrivalForStop`)
  misses ~65% of last-stop arrivals because buses' final GPS often
  projects just shy of the last stop's projected distance (layover, GPS
  multipath, projection variance). The fallback walks backward from the
  last stop and attributes the bus's max-progress probe TS as approximate
  arrival for any trailing stop within tolerance. **It must run only at
  finalization** — applying it during in-flight tracking would prematurely
  mark a trip "completed" while the bus is still en route. The function
  is `applyTrailingStopFallback`. Don't move the call out of `tripToRows`
  without preserving the finalization-only invariant.

- **Public GCS objects default to `Cache-Control: public, max-age=3600`.**
  `latest.json` and friends will appear stale via the public URL even right
  after a fresh write. Set `w.CacheControl` in `writeObject`
  (`cmd/scraper/main.go`) for any object that needs to be fresh.

- **Custom metric writes occasionally return gRPC `Internal` (code 13).**
  This is transient backend flakiness, not a schema/quota problem. The
  warning log is appropriate; don't add elaborate retry logic unless the
  rate climbs above ~1/day.

- **`make deploy` without `TAG=` rolls the image back to `v1`.** Always
  pass an explicit tag.

- **`gcloud logging read --freshness=Xm` window is the only way to scope
  Cloud Run logs efficiently.** Without it you'll wait minutes and time out.

- **Cross-account read of `gs://ac-transit/maptime/`.** The backfill source
  bucket is owned by a separate account (the long-running val.town-era
  scraper). The `actransit-scraper` SA needs `roles/storage.objectViewer`
  on that bucket — granted manually, *not* in this repo's Terraform.
  Don't add it to `iam.tf`; the bucket isn't in this project.

## Verify before recommending

If you're about to recommend an action based on memory or prior context:

- For a file path: confirm it exists.
- For a function/flag: grep for it.
- For "the metric is healthy" or "the cron is firing": query the time series
  API or `gcloud logging read`. The dashboard can lie; the API doesn't.

"It used to work" is not the same as "it works now."
