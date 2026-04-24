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
make tf-init             # only first time
make build TAG=v1        # build + push image via Cloud Build
make deploy TAG=v1       # terraform apply with the new tag
make invoke              # curl /scrape with your identity token
make logs                # recent Cloud Run logs
```

## Cost

Designed for ~$1/month on a personal GCP project (free tier covers
Cloud Run invocations and Scheduler; main line items are GCS write
ops and Secret Manager accesses).
