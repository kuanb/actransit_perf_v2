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
  main.go             Routes, GCS read/write, /scrape, /refresh-stops, /refresh-gtfs
  track.go            /track-performance handler + metric emit + state machine
  bq.go               BigQuery row construction and inserts
  gtfs_static.go      Static GTFS download + processing
  gtfs_cache.go       In-process cache of processed static GTFS
  geo.go, stats.go    Shape projection and trackStats type
infra/              Terraform (state in gs://transit-203605-tfstate)
  scraper.tf          Cloud Run service + revision
  schedule.tf         Cloud Scheduler jobs (cron, OIDC invoker)
  bigquery.tf         Datasets + tables
  storage.tf          GCS buckets
  alerting.tf         Email alert policies
  dashboard.tf        Cloud Monitoring dashboard JSON
  iam.tf              Service accounts and bindings
docs/               Architecture and data-model design docs
site/               Static frontend
Makefile            Authoritative entry points — see README.md "Deploy"
```

## Cron jobs (all run via Cloud Scheduler with OIDC)

| Endpoint              | Cron        | Side effects                                   |
|-----------------------|-------------|------------------------------------------------|
| `/scrape`             | `* * * * *` | Writes `latest.json`, appends to `history.json` |
| `/track-performance`  | `* * * * *` | Reads `latest.json` + `state.json`, writes `state.json`, finalizes trips into BQ, emits two custom metrics |
| `/refresh-stops`      | `0 */6 * * *` | Writes `route_stops.json` |
| `/refresh-gtfs`       | `0 22 * * *` | Refreshes `gtfs/current.zip` |

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

- **`/track-performance` reads then writes `state.json` with a generation
  precondition.** If two invocations overlap (Scheduler retry on a slow
  request, or `containerConcurrency > 1`), the second hits `errStateConflict`
  at `track.go:147` and **early-returns before the metric emit**. Sparse
  custom-metric data is usually a downstream symptom of this loop, not a
  Monitoring problem.

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

## Verify before recommending

If you're about to recommend an action based on memory or prior context:

- For a file path: confirm it exists.
- For a function/flag: grep for it.
- For "the metric is healthy" or "the cron is firing": query the time series
  API or `gcloud logging read`. The dashboard can lie; the API doesn't.

"It used to work" is not the same as "it works now."
