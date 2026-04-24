resource "google_storage_bucket" "cache" {
  name                        = "${var.project_id}-actransit-cache"
  location                    = var.region
  storage_class               = "STANDARD"
  force_destroy               = true
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      num_newer_versions = 1
    }
    action {
      type = "Delete"
    }
  }
}
