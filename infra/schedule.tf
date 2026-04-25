resource "google_service_account" "scheduler" {
  account_id   = "actransit-scheduler"
  display_name = "AC Transit Cloud Scheduler invoker"
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = google_cloud_run_v2_service.scraper.project
  location = google_cloud_run_v2_service.scraper.location
  name     = google_cloud_run_v2_service.scraper.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "scrape" {
  name             = "actransit-scrape"
  region           = var.region
  schedule         = "* * * * *"
  time_zone        = "Etc/UTC"
  attempt_deadline = "60s"

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.scraper.uri}/scrape"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.scraper.uri
    }
  }
}

resource "google_cloud_scheduler_job" "refresh_stops" {
  name             = "actransit-refresh-stops"
  region           = var.region
  schedule         = "0 */6 * * *"
  time_zone        = "Etc/UTC"
  attempt_deadline = "60s"

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.scraper.uri}/refresh-stops"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.scraper.uri
    }
  }
}

resource "google_cloud_scheduler_job" "refresh_gtfs" {
  name             = "actransit-refresh-gtfs"
  region           = var.region
  schedule         = "0 22 * * *"
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "60s"

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.scraper.uri}/refresh-gtfs"

    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloud_run_v2_service.scraper.uri
    }
  }
}
