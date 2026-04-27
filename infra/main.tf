provider "google" {
  project = var.project_id
  region  = var.region

  # Without these, the provider routes some API calls (notably
  # billingbudgets) through a Google-internal fallback project that
  # nothing on this account has access to, and creation fails with
  # SERVICE_DISABLED for "consumer: 764086051850". user_project_override
  # tells the provider to bill API calls to billing_project instead.
  billing_project       = var.project_id
  user_project_override = true
}
