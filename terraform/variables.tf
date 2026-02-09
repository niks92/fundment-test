variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "europe-west2"
}

variable "environment" {
  description = "Environment name (dev, ci, prod)"
  type        = string
  default     = "dev"
}

variable "data_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU" # EU multi-region for data residency compliance
}
