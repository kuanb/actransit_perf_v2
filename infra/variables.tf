variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "Default GCP region for regional resources"
  default     = "us-west1"
}

variable "alert_email" {
  type        = string
  description = "Email address for error alerts"
}

variable "image_tag" {
  type        = string
  description = "Tag of the scraper container image in Artifact Registry"
  default     = "v1"
}

variable "billing_account_id" {
  type        = string
  description = "Cloud Billing account ID that owns the project (e.g. 0106AD-BA7AD7-261A86); used for budget alerts."
}
