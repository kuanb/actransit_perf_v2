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
}

variable "billing_account_id" {
  type        = string
  description = "Cloud Billing account ID that owns the project (e.g. 0106AD-BA7AD7-261A86); used for budget alerts."
}

variable "analysis_enabled" {
  type        = bool
  description = "Public kill switch for the read-only analysis interface"
  default     = false
}

variable "analysis_gemini_model" {
  type        = string
  description = "Vertex AI Gemini model used for bounded tool selection and response writing"
  default     = "gemini-3.5-flash-lite"
}

variable "analysis_allowed_origins" {
  type        = list(string)
  description = "Browser origins allowed to call the public analysis API"
  default     = ["https://ac-transit.com", "https://kuanb.github.io", "http://localhost:8000", "http://127.0.0.1:8000"]
}

variable "analysis_cooldown_seconds" {
  type        = number
  description = "Minimum delay between accepted questions from one browser session"
  default     = 20

  validation {
    condition     = var.analysis_cooldown_seconds >= 5
    error_message = "analysis_cooldown_seconds must be at least 5."
  }
}

variable "analysis_max_active_queries" {
  type        = number
  description = "Maximum globally active analysis requests"
  default     = 2

  validation {
    condition     = var.analysis_max_active_queries >= 1 && var.analysis_max_active_queries <= 4
    error_message = "analysis_max_active_queries must be between 1 and 4."
  }
}

variable "analysis_max_session_daily" {
  type        = number
  description = "Maximum accepted questions per anonymous browser session per Pacific day"
  default     = 10
}

variable "analysis_max_global_daily" {
  type        = number
  description = "Maximum accepted questions across the public service per Pacific day"
  default     = 60
}

variable "analysis_max_prompt_chars" {
  type        = number
  description = "Maximum public question length"
  default     = 500
}

variable "analysis_max_follow_ups" {
  type        = number
  description = "Maximum clarifying follow-ups after an initial public analysis answer"
  default     = 3

  validation {
    condition     = var.analysis_max_follow_ups >= 0 && var.analysis_max_follow_ups <= 3
    error_message = "analysis_max_follow_ups must be between 0 and 3."
  }
}

variable "analysis_bq_maximum_bytes_billed" {
  type        = number
  description = "BigQuery maximum bytes billed for each MCP query"
  default     = 104857600
}
