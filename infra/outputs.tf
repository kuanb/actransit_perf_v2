output "artifact_registry_repo" {
  description = "Full path to the Artifact Registry Docker repo"
  value       = "${google_artifact_registry_repository.scraper.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.scraper.repository_id}"
}

output "cache_bucket" {
  description = "Name of the GCS cache bucket"
  value       = google_storage_bucket.cache.name
}

output "scraper_service_account" {
  description = "Email of the Cloud Run service account"
  value       = google_service_account.scraper.email
}

output "scraper_url" {
  description = "Cloud Run service URL"
  value       = google_cloud_run_v2_service.scraper.uri
}
