# actransit_perf_v2

Minutely AC Transit GTFS-Realtime vehicle scraper running on Google Cloud.
Migrated from val.town as a learning project for Go and GCP.

## What it does

Every minute, Cloud Scheduler pings a Cloud Run service (`/scrape`).
The service fetches the AC Transit GTFS-Realtime vehicle feed, decodes
the protobuf, and writes two JSON objects to a GCS bucket:

- `latest.json` — vehicle entities from the most recent fetch
- `history.json` — last 10 snapshots of `latest.json`

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
make build TAG=vN           # build + push image to Artifact Registry
make deploy TAG=vN          # terraform apply with the new tag
make invoke                 # curl /scrape with your identity token
make logs                   # recent Cloud Run logs
```

### Versioning

The Go scraper is shipped as a container image tagged `vN` (`v1`, `v2`, ...) in
Artifact Registry. The tag is just an integer marker — there's no semver, no
automation. Bump it manually whenever the image content needs to change:

| Change                                  | Bump tag? |
|-----------------------------------------|-----------|
| Go code (`cmd/`, future `internal/`)    | yes       |
| `Dockerfile` or `go.mod` / `go.sum`     | yes       |
| Terraform only (`infra/*.tf`)           | no        |

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
