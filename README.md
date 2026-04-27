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

A third job (`/refresh-gtfs`) runs nightly at 22:00 America/Los_Angeles,
downloads the static GTFS zip, hashes it, and writes a new dated archive
plus updates `gtfs/current.zip` only if the hash changed:

- `gtfs/current.zip` — the most recent GTFS, also tagged with the SHA-256 in metadata
- `gtfs/<YYYYMMDDTHHMMSSZ>.zip` — immutable dated archives of each version observed

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
| Current GTFS     | `https://storage.googleapis.com/transit-203605-actransit-cache/gtfs/current.zip`          |

## Stack

- **Compute:** Cloud Run (Go 1.25, distroless image)
- **Schedule:** Cloud Scheduler → HTTPS + OIDC
- **Cache:** Cloud Storage (GCS)
- **Secret:** Secret Manager (`actransit-token`)
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

Run `make test` to execute unit tests locally (no external dependencies).
After a deploy, `make smoke` verifies the live service end-to-end via curl
and `gsutil`. A pre-commit hook is available — install with
`make hooks-install` to auto-run `go vet` + `go test` before every commit.
GitHub Actions runs the same checks on every push to `main`. See
[`docs/testing.md`](docs/testing.md) for what each tier covers and what's
intentionally not unit-tested.

## Cost

Designed for ~$1/month on a personal GCP project (free tier covers
Cloud Run invocations and Scheduler; main line items are GCS write
ops and Secret Manager accesses).
