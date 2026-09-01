resource "google_bigquery_dataset" "actransit" {
  dataset_id                 = "actransit"
  location                   = var.region
  description                = "AC Transit performance analytics"
  delete_contents_on_destroy = false
}

resource "google_bigquery_table" "trip_observations" {
  dataset_id          = google_bigquery_dataset.actransit.dataset_id
  table_id            = "trip_observations"
  description         = "One row per (trip_id, stop_sequence) — schedule vs. actual + per-leg metrics"
  deletion_protection = true

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
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "service_date"
  }

  clustering = ["route_id", "trip_id"]

  schema = file("${path.module}/schemas/trip_probes.json")
}

resource "google_bigquery_table" "ridership_observations" {
  dataset_id          = google_bigquery_dataset.actransit.dataset_id
  table_id            = "ridership_observations"
  description         = "One row per active vehicle per minute from AC Transit realtime APC attributes"
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "service_date"
  }

  clustering = ["route_id", "vehicle_id"]

  schema = file("${path.module}/schemas/ridership_observations.json")
}

resource "google_bigquery_table" "api_request_observations" {
  dataset_id          = google_bigquery_dataset.actransit.dataset_id
  table_id            = "api_request_observations"
  description         = "One row per scheduled AC Transit vehicle-location or ridership API request"
  deletion_protection = true

  time_partitioning {
    type          = "DAY"
    field         = "service_date"
    expiration_ms = 7776000000
  }

  clustering = ["source", "outcome"]

  schema = file("${path.module}/schemas/api_request_observations.json")
}

resource "google_bigquery_table" "api_request_hourly" {
  dataset_id          = google_bigquery_dataset.actransit.dataset_id
  table_id            = "api_request_hourly"
  description         = "Permanent hourly and daily API request health aggregates"
  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "service_date"
  }

  clustering = ["source", "is_total"]

  schema = file("${path.module}/schemas/api_request_hourly.json")
}

resource "google_bigquery_dataset_iam_member" "scraper_writer" {
  dataset_id = google_bigquery_dataset.actransit.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.scraper.email}"
}

# /generate-daily-stats issues SELECT queries against trip_observations.
# bigquery.dataEditor covers reads/writes on table data, but query jobs
# require bigquery.jobUser at the project level.
resource "google_project_iam_member" "scraper_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.scraper.email}"
}
