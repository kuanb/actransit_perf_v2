locals {
  image_url = "${google_artifact_registry_repository.scraper.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.scraper.repository_id}/scraper:${var.image_tag}"
}

resource "google_cloud_run_v2_service" "scraper" {
  name     = "actransit-scraper"
  location = var.region

  template {
    service_account = google_service_account.scraper.email
    timeout         = "60s"

    scaling {
      max_instance_count = 2
    }

    containers {
      image = local.image_url

      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        cpu_idle = true
      }

      env {
        name  = "CACHE_BUCKET"
        value = google_storage_bucket.cache.name
      }

      env {
        name  = "SECRET_NAME"
        value = "${data.google_secret_manager_secret.actransit_token.id}/versions/latest"
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "developer_invoker" {
  project  = google_cloud_run_v2_service.scraper.project
  location = google_cloud_run_v2_service.scraper.location
  name     = google_cloud_run_v2_service.scraper.name
  role     = "roles/run.invoker"
  member   = "user:${var.alert_email}"
}
