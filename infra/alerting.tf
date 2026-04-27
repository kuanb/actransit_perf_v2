resource "google_monitoring_notification_channel" "email" {
  display_name = "Personal email"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }
}

# Spend cap. Catches accidental cost spikes (a runaway BQ query, a
# backfill stuck in a loop, an enabled-by-mistake API). Alerts at
# 50% / 90% / 100% of the monthly budget; default-IAM recipients
# disabled so the only notification path is our existing email channel
# (avoids dupes since the project owner == alert_email anyway). Requires
# billingbudgets.googleapis.com enabled on the project — one time:
#   gcloud services enable billingbudgets.googleapis.com --project=<id>
resource "google_billing_budget" "monthly_cap" {
  billing_account = var.billing_account_id
  display_name    = "actransit project monthly budget"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = "5"
    }
  }

  threshold_rules { threshold_percent = 0.5 }
  threshold_rules { threshold_percent = 0.9 }
  threshold_rules { threshold_percent = 1.0 }

  all_updates_rule {
    monitoring_notification_channels = [google_monitoring_notification_channel.email.id]
    disable_default_iam_recipients   = true
  }
}

resource "google_monitoring_alert_policy" "scraper_5xx" {
  display_name = "actransit-scraper 5xx errors"
  combiner     = "OR"

  conditions {
    display_name = "Cloud Run 5xx rate > 0"

    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = \"actransit-scraper\" AND metric.type = \"run.googleapis.com/request_count\" AND metric.labels.response_code_class = \"5xx\""
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
