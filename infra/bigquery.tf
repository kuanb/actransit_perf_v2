resource "google_bigquery_dataset" "actransit" {
  dataset_id                 = "actransit"
  location                   = var.region
  description                = "AC Transit performance analytics"
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "trip_observations" {
  dataset_id          = google_bigquery_dataset.actransit.dataset_id
  table_id            = "trip_observations"
  description         = "One row per (trip_id, stop_sequence) — schedule vs. actual + per-leg metrics"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "service_date"
  }

  clustering = ["route_id", "trip_id"]

  schema = file("${path.module}/schemas/trip_observations.json")
}

resource "google_bigquery_table" "trip_probes" {
  dataset_id          = google_bigquery_dataset.actransit.dataset_id
  table_id            = "trip_probes"
  description         = "One row per probe observation — projected onto route, audit trail for trip replay"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "service_date"
  }

  clustering = ["route_id", "trip_id"]

  schema = file("${path.module}/schemas/trip_probes.json")
}

resource "google_bigquery_dataset_iam_member" "scraper_writer" {
  dataset_id = google_bigquery_dataset.actransit.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.scraper.email}"
}
