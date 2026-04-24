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
