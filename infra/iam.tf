resource "google_service_account" "scraper" {
  account_id   = "actransit-scraper"
  display_name = "AC Transit scraper Cloud Run identity"
}

data "google_secret_manager_secret" "actransit_token" {
  secret_id = "actransit-token"
}

data "google_secret_manager_secret" "actransit_gtfs_token" {
  secret_id = "actransit-gtfs-token"
}

resource "google_secret_manager_secret_iam_member" "scraper_token_access" {
  secret_id = data.google_secret_manager_secret.actransit_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.scraper.email}"
}

resource "google_secret_manager_secret_iam_member" "scraper_gtfs_token_access" {
  secret_id = data.google_secret_manager_secret.actransit_gtfs_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.scraper.email}"
}

resource "google_storage_bucket_iam_member" "scraper_cache_access" {
  bucket = google_storage_bucket.cache.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.scraper.email}"
}

resource "google_storage_bucket_iam_member" "public_read" {
  bucket = google_storage_bucket.cache.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

resource "google_project_iam_member" "scraper_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.scraper.email}"
}
