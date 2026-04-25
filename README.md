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
make release TAG=vN         # build + push image, then terraform apply (use this for code changes)
make build TAG=vN           # just build + push (rare; use release instead)
make deploy TAG=vN          # just terraform apply at a known-good tag (for infra-only changes)
make invoke                 # curl /scrape with your identity token
make logs                   # recent Cloud Run logs
```

`make deploy` refuses to run if the tag isn't in Artifact Registry, so you
can't accidentally deploy a tag whose build hasn't finished pushing.

### Versioning

The Go scraper is shipped as a container image tagged `vN` (`v1`, `v2`, ...) in
Artifact Registry. The tag is just an integer marker — there's no semver, no
automation. Bump it manually whenever the image content needs to change:

| Change                                  | Command                       |
|-----------------------------------------|-------------------------------|
| Go code (`cmd/`, future `internal/`)    | `make release TAG=vN`         |
| `Dockerfile` or `go.mod` / `go.sum`     | `make release TAG=vN`         |
| Terraform only (`infra/*.tf`)           | `make deploy TAG=<current>`   |

For pure Terraform changes, pass the **current** tag (don't omit `TAG=`) so
Terraform doesn't silently downgrade the image to the variable's default (`v1`).

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
make deploy TAG=v1
```

Cloud Run will roll a new revision pointing at the old image. The faster
console-only path (no Terraform involved) is `gcloud run services
update-traffic actransit-scraper --region us-west1 --to-revisions=<rev>=100` —
flips traffic to a previously deployed revision instantly, but Terraform state
will then disagree until you reconcile.

## Cost

Designed for ~$1/month on a personal GCP project (free tier covers
Cloud Run invocations and Scheduler; main line items are GCS write
ops and Secret Manager accesses).
