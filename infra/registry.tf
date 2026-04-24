resource "google_artifact_registry_repository" "scraper" {
  location      = var.region
  repository_id = "actransit-scraper"
  description   = "Container images for the AC Transit scraper"
  format        = "DOCKER"
}
