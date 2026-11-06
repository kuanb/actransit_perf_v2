locals {
  analysis_agent_image_url = "${google_artifact_registry_repository.scraper.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.scraper.repository_id}/analysis-agent:${var.image_tag}"
  analysis_mcp_image_url   = "${google_artifact_registry_repository.scraper.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.scraper.repository_id}/analysis-data-mcp:${var.image_tag}"
}

resource "google_project_service" "analysis" {
  for_each = toset([
    "aiplatform.googleapis.com",
    "firestore.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_firestore_database" "analysis" {
  project                 = var.project_id
  name                    = "(default)"
  location_id             = var.region
  type                    = "FIRESTORE_NATIVE"
  delete_protection_state = "DELETE_PROTECTION_ENABLED"
  deletion_policy         = "ABANDON"

  depends_on = [google_project_service.analysis]
}

resource "google_firestore_field" "analysis_result_expiry" {
  project    = var.project_id
  database   = google_firestore_database.analysis.name
  collection = "analysis_results"
  field      = "expires_at"

  ttl_config {}
}

resource "google_service_account" "analysis_agent" {
  account_id   = "actransit-analysis-agent"
  display_name = "AC Transit public analysis agent"
}

resource "google_service_account" "analysis_mcp" {
  account_id   = "actransit-analysis-mcp"
  display_name = "AC Transit read-only analysis MCP"
}

resource "google_cloud_run_v2_service" "analysis_mcp" {
  name     = "actransit-analysis-mcp"
  location = var.region

  template {
    service_account                  = google_service_account.analysis_mcp.email
    timeout                          = "30s"
    max_instance_request_concurrency = 2

    scaling {
      max_instance_count = 1
    }

    containers {
      image = local.analysis_mcp_image_url

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle = true
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "CACHE_BUCKET"
        value = google_storage_bucket.cache.name
      }

      env {
        name  = "BQ_LOCATION"
        value = var.region
      }

      env {
        name  = "BQ_MAXIMUM_BYTES_BILLED"
        value = tostring(var.analysis_bq_maximum_bytes_billed)
      }
    }
  }

  depends_on = [
    google_bigquery_table.trip_observations,
  ]
}

resource "google_cloud_run_v2_service" "analysis_agent" {
  name     = "actransit-analysis-agent"
  location = var.region

  template {
    service_account                  = google_service_account.analysis_agent.email
    timeout                          = "60s"
    max_instance_request_concurrency = 4

    scaling {
      max_instance_count = 1
    }

    containers {
      image = local.analysis_agent_image_url

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle = true
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "MCP_URL"
        value = "${google_cloud_run_v2_service.analysis_mcp.uri}/mcp"
      }

      env {
        name  = "MCP_AUDIENCE"
        value = google_cloud_run_v2_service.analysis_mcp.uri
      }

      env {
        name  = "GEMINI_MODEL"
        value = var.analysis_gemini_model
      }

      env {
        name  = "VERTEX_LOCATION"
        value = "global"
      }

      env {
        name  = "ALLOWED_ORIGINS"
        value = join(",", var.analysis_allowed_origins)
      }

      env {
        name  = "ANALYSIS_ENABLED"
        value = tostring(var.analysis_enabled)
      }

      env {
        name  = "COOLDOWN_SECONDS"
        value = tostring(var.analysis_cooldown_seconds)
      }

      env {
        name  = "MAX_ACTIVE_QUERIES"
        value = tostring(var.analysis_max_active_queries)
      }

      env {
        name  = "MAX_SESSION_DAILY"
        value = tostring(var.analysis_max_session_daily)
      }

      env {
        name  = "MAX_GLOBAL_DAILY"
        value = tostring(var.analysis_max_global_daily)
      }

      env {
        name  = "MAX_PROMPT_CHARS"
        value = tostring(var.analysis_max_prompt_chars)
      }

      env {
        name  = "MAX_FOLLOW_UPS"
        value = tostring(var.analysis_max_follow_ups)
      }

      env {
        name  = "CHAT_LOG_BUCKET"
        value = google_storage_bucket.analysis_chats.name
      }

      env {
        name  = "PUBLIC_CHAT_BUCKET"
        value = google_storage_bucket.cache.name
      }

      env {
        name  = "MAX_TOOL_CALLS"
        value = "4"
      }

      env {
        name  = "MAX_RESPONSE_BYTES"
        value = tostring(256 * 1024)
      }

      env {
        name  = "LEASE_SECONDS"
        value = "90"
      }
    }
  }

  depends_on = [google_firestore_database.analysis]
}

resource "google_cloud_run_v2_service_iam_member" "analysis_public" {
  project  = google_cloud_run_v2_service.analysis_agent.project
  location = google_cloud_run_v2_service.analysis_agent.location
  name     = google_cloud_run_v2_service.analysis_agent.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_v2_service_iam_member" "analysis_agent_mcp_invoker" {
  project  = google_cloud_run_v2_service.analysis_mcp.project
  location = google_cloud_run_v2_service.analysis_mcp.location
  name     = google_cloud_run_v2_service.analysis_mcp.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.analysis_agent.email}"
}

resource "google_project_iam_member" "analysis_agent_vertex" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.analysis_agent.email}"
}

resource "google_project_iam_member" "analysis_agent_firestore" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = "serviceAccount:${google_service_account.analysis_agent.email}"
}

resource "google_storage_bucket_iam_member" "analysis_agent_chat_writer" {
  bucket = google_storage_bucket.analysis_chats.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.analysis_agent.email}"
}

resource "google_storage_bucket_iam_member" "analysis_agent_public_chat_writer" {
  bucket = google_storage_bucket.cache.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.analysis_agent.email}"

  condition {
    title      = "analysis_public_chat_prefix"
    expression = "resource.name.startsWith(\"projects/_/buckets/${google_storage_bucket.cache.name}/objects/analysis/public-chats/\")"
  }
}

resource "google_storage_bucket_iam_member" "analysis_mcp_cache_reader" {
  bucket = google_storage_bucket.cache.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.analysis_mcp.email}"
}

resource "google_bigquery_dataset_iam_member" "analysis_mcp_reader" {
  dataset_id = google_bigquery_dataset.actransit.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.analysis_mcp.email}"
}

resource "google_project_iam_member" "analysis_mcp_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.analysis_mcp.email}"
}

resource "google_storage_bucket_object" "analysis_config" {
  name          = "analysis/config.json"
  bucket        = google_storage_bucket.cache.name
  content_type  = "application/json"
  cache_control = "no-store, max-age=0"
  content = jsonencode({
    api_url            = google_cloud_run_v2_service.analysis_agent.uri
    cooldown_seconds   = var.analysis_cooldown_seconds
    max_active_queries = var.analysis_max_active_queries
    max_follow_ups     = var.analysis_max_follow_ups
    max_prompt_chars   = var.analysis_max_prompt_chars
    enabled            = var.analysis_enabled
  })
}

resource "google_monitoring_alert_policy" "analysis_agent_5xx" {
  display_name = "actransit analysis agent 5xx errors"
  combiner     = "OR"

  conditions {
    display_name = "Analysis agent 5xx rate > 0"

    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"actransit-analysis-agent\" AND metric.type = \"run.googleapis.com/request_count\" AND metric.labels.response_code_class = \"5xx\""
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_RATE"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.id]

  alert_strategy {
    auto_close = "604800s"
  }
}
