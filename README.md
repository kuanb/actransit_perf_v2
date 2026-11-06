# actransit_perf_v2

Minutely AC Transit GTFS-Realtime vehicle scraper running on Google Cloud.
Migrated from val.town as a learning project for Go and GCP.

## What it does

Every minute, Cloud Scheduler pings a Cloud Run service (`/scrape`).
The service fetches the AC Transit GTFS-Realtime vehicle feed, decodes
the protobuf, and writes two JSON objects to a GCS bucket:

- `latest.json` — vehicle entities from the most recent fetch
- `history.json` — last 10 snapshots of `latest.json`

A second job (`/refresh-stops`) runs every 6 hours, deriving the unique
route IDs from `latest.json` and fetching the stops list for each route
from AC Transit's `/allstops` endpoint, written to a third object:

- `route_stops.json` — `[{routeName, processedStops}]` for each active route

Another minutely job (`/scrape-ridership`) fetches AC Transit's realtime APC
attributes into an independent, append-only BigQuery table. It also publishes a
small live snapshot with a rolling 24-hour series for the public ridership page:

- `ridership/latest.json` — current vehicles, crowding fields, transparent rider estimates, and one systemwide summary per minute for the last 24 hours

Both minutely upstream requests also write one health observation to BigQuery,
including latency, HTTP status, response size, and a categorized failure outcome
for timeouts, 4xx/5xx responses, transport errors, and invalid payloads. Raw
request rows expire after 90 days; permanent hourly and daily summaries are
generated with each daily report.

A third job (`/refresh-gtfs`) runs nightly at 22:00 America/Los_Angeles,
downloads the static GTFS zip, hashes it, and writes a new dated archive
plus updates `gtfs/current.zip` only if the hash changed:

- `gtfs/current.zip` — the most recent GTFS, also tagged with the SHA-256 in metadata
- `gtfs/<YYYYMMDDTHHMMSSZ>.zip` — immutable dated archives of each version observed

The performance tracker writes completed trips to BigQuery. Daily, weekly,
and monthly generators combine those observations with the GTFS schedule and
publish report JSON under `stats/`. The agency-comparison fields reproduce AC
Transit's Service Operated and On-Time Performance definitions from independent
data, including route-level weekday/weekend time-of-day breakdowns. A daily
`/refresh-published-kpis` job imports AC Transit's published monthly values for
side-by-side comparison.

The daily generator also publishes a small school-service report for El Cerrito
High and Korematsu Middle. It compares every expected morning trip with a 250 m
school-arrival geofence, preserves the effective-dated school start time used
for that day, and reports confirmed origin departures more than 15 minutes late.
Reports are limited to weekdays within each school's effective-dated academic
calendar and exclude district holidays, recesses, and secondary no-school days.

The report card and route pages also link to an experimental read-only analysis
interface. A public Cloud Run agent uses Vertex AI to select from a fixed set of
private MCP tools; those tools can read published report JSON, archived GTFS,
and bounded BigQuery partitions, but cannot accept SQL or write project data.
Responses carry structured text, charts, tables, data sources, and the exact
timeframe used.

Route comparisons are segmented by service type. Open-ended rankings default
to regular Local/Rapid/Tempo service; 600-series Supplementary School Service,
800-series All Nighter service, and lettered Transbay routes are ranked only
within their own groups. The MCP rejects cross-group comparisons and includes
the service type and official AC Transit classification source in structured
results.

## Design docs

In-progress / planned services have architecture artifacts under `docs/`:

- [`docs/architecture.md`](docs/architecture.md) — vehicle performance tracking service (planned)
- [`docs/data-model.md`](docs/data-model.md) — state, processed GTFS, and BigQuery schemas (planned)

## Public endpoints

The cache bucket is world-readable so frontends can fetch the JSON
directly via HTTPS, with permissive CORS for browsers:

| Resource         | URL                                                                                       |
|------------------|-------------------------------------------------------------------------------------------|
| Latest vehicles  | `https://storage.googleapis.com/transit-203605-actransit-cache/latest.json`               |
| Vehicle history  | `https://storage.googleapis.com/transit-203605-actransit-cache/history.json`              |
| Route stops      | `https://storage.googleapis.com/transit-203605-actransit-cache/route_stops.json`          |
| Live ridership   | `https://storage.googleapis.com/transit-203605-actransit-cache/ridership/latest.json`     |
| Current GTFS     | `https://storage.googleapis.com/transit-203605-actransit-cache/gtfs/current.zip`          |
| Latest daily report | `https://storage.googleapis.com/transit-203605-actransit-cache/stats/latest.json`       |
| Latest weekly report | `https://storage.googleapis.com/transit-203605-actransit-cache/stats/weekly/latest.json` |
| Latest monthly KPI report | `https://storage.googleapis.com/transit-203605-actransit-cache/stats/monthly/latest.json` |
| Published AC Transit KPIs | `https://storage.googleapis.com/transit-203605-actransit-cache/stats/published-kpis/latest.json` |
| Latest school report | `https://storage.googleapis.com/transit-203605-actransit-cache/stats/schools/latest.json` |
| Analysis UI config | `https://storage.googleapis.com/transit-203605-actransit-cache/analysis/config.json` |

## Stack

- **Compute:** Cloud Run (Go 1.25 scraper; Python 3.13 analysis agent and MCP)
- **Schedule:** Cloud Scheduler → HTTPS + OIDC
- **Cache:** Cloud Storage (GCS)
- **Analysis:** Vertex AI Gemini + FastMCP + Firestore rate limits
- **Warehouse:** BigQuery with partition pruning and per-query byte caps
- **Secrets:** Secret Manager (`actransit-token`, `actransit-gtfs-token`, `actransit-ridership-token`)
- **Alerts:** Cloud Monitoring → email
- **IaC:** Terraform (state in GCS)

## Deploy

One-time prerequisites: `gcloud auth login`, `gcloud auth application-default login`,
required APIs enabled, and a Terraform state bucket at
`gs://transit-203605-tfstate`.

```sh
make tf-init                # only the first time
make release                # build + deploy at HEAD's git SHA (use this for code changes)
make deploy                 # terraform apply at the currently-running tag (infra-only changes)
make build                  # just build (rare; use release instead)
make invoke                 # curl /scrape with your identity token
make logs                   # recent Cloud Run logs
make analysis-logs          # recent analysis-agent and MCP logs
make backfill-agency-kpis-history  # rebuild KPI comparison history after deployment
make backfill-school-stats-history # build school reports for every indexed service date
```

`make deploy` refuses to run if the tag isn't in Artifact Registry, so you
can't accidentally deploy a tag whose build hasn't finished pushing.

### Versioning

Image tags are auto-derived from the current git short SHA (e.g.
`abc1234`). Every commit ships under a unique tag — no semver, no
manual bumping, no chance of re-tagging an old SHA and Cloud Run silently
keeping the prior digest. If the working tree is dirty, the tag is
suffixed `-dirty` and `make release` prompts before building it.

| Change                                  | Command           |
|-----------------------------------------|-------------------|
| Go code (`cmd/`, future `internal/`)    | `make release`    |
| Analysis code (`services/`)             | `make release`    |
| `Dockerfile` or `go.mod` / `go.sum`     | `make release`    |
| Terraform only (`infra/*.tf`)           | `make deploy`     |

`make deploy` queries Cloud Run for the currently-running image and
applies Terraform against that tag, so it physically can't downgrade or
default to `v1`. Override either default with `TAG=` (e.g.
`make release TAG=hotfix1` for a friendly name).

### Finding the current version

```sh
# What's actually deployed right now:
gcloud run services describe actransit-scraper --region us-west1 \
  --format='value(spec.template.spec.containers[].image)'

# All tags in Artifact Registry (newest last):
gcloud artifacts docker tags list \
  us-west1-docker.pkg.dev/transit-203605/actransit-scraper/scraper
```

### Rollback

Old tags stay in Artifact Registry. Roll back by deploying a prior tag:

```sh
make deploy TAG=<prior-sha>
```

Cloud Run will roll a new revision pointing at the old image. The faster
console-only path (no Terraform involved) is `gcloud run services
update-traffic actransit-scraper --region us-west1 --to-revisions=<rev>=100` —
flips traffic to a previously deployed revision instantly, but Terraform state
will then disagree until you reconcile.

## Testing

Run `make test` to execute Go and analysis-policy unit tests locally (no GCP
credentials or network calls).
The optional live model regression suite uses five rate-limited questions and
is intentionally not part of CI:

```sh
make eval-analysis-route-groups ANALYSIS_API_URL=https://actransit-analysis-agent-komsnvluna-uw.a.run.app
make analysis-eval-logs
```

The service logs only prompt feature tags, bounded tool arguments, and policy
outcomes—not raw visitor prompts. The checked-in synthetic cases preserve the
exact category-sensitive phrasings used to detect model/tool-selection drift.
After a deploy, `make smoke` verifies the live service end-to-end via curl
and `gsutil`. A pre-commit hook is available — install with
`make hooks-install` to auto-run `go vet` + `go test` before every commit.
GitHub Actions runs the same checks on every push to `main`. See
[`docs/testing.md`](docs/testing.md) for what each tier covers and what's
intentionally not unit-tested.

## Cost

The existing collection and reporting path is designed for roughly $1–2/month.
For light public-analysis use, expect approximately another **$0.50–$3.50/month**:
Cloud Run, Firestore, and BigQuery should remain in their free tiers, while
Gemini 3.5 Flash-Lite tokens are the main incremental cost. This assumes roughly
10–30 questions/day at about 2,000–6,000 input tokens and 300–700 output tokens
per question across the tool-calling turns. The configured shared cap of 60
accepted questions/day bounds a busier month to roughly **$3–$7**, depending on
tool-result size. These are planning estimates, not a billing guarantee.

The public defaults are one question every 20 seconds per browser, at most two
active questions, 10 accepted questions per browser/day, 60 accepted questions
globally/day, three clarifying follow-ups after an initial answer, four tool
calls/question, 500 prompt characters, and 100 MiB maximum BigQuery bytes billed
per query. Follow-ups retain only the bounded prior question, narrative answer,
and timeframe for one hour; every turn consumes the normal cooldown and daily
allowances. All are Terraform variables except the tool-call and response-size
caps, which are service environment settings.
The Terraform default leaves the public kill switch off for the first deploy;
enable it only after the private MCP and route-scoped browser flow pass smoke
checks.
