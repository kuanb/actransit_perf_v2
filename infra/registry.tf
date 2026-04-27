resource "google_artifact_registry_repository" "scraper" {
  location      = var.region
  repository_id = "actransit-scraper"
  description   = "Container images for the AC Transit scraper"
  format        = "DOCKER"

  # Cleanup policies layer: KEEP wins over DELETE, so a tag older than 30
  # days that's still in the most-recent-10 set will not be deleted.
  # Anything older than 30 days AND outside the recent-10 gets pruned by
  # Artifact Registry's daily cleanup job. Rebuilds from old SHAs are
  # always possible via `make release TAG=<old-sha>`.
  cleanup_policies {
    id     = "keep-recent-10"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  cleanup_policies {
    id     = "delete-older-than-30d"
    action = "DELETE"
    condition {
      older_than = "2592000s" # 30 days
    }
  }
}
