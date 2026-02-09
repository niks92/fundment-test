# -----------------------------------------------------------------------------
# BigQuery Datasets
# Three-layer architecture: raw -> staging -> reporting
# -----------------------------------------------------------------------------

# Raw layer: Source data ingested from GCS via Python pipeline
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "raw"
  friendly_name = "Raw Data"
  description   = "Source data ingested from GCS. Loaded via Python pipeline (pipelines/ingest_fees.py)."
  location      = var.data_location
  project       = var.project_id

  labels = {
    environment = var.environment
    layer       = "raw"
    managed_by  = "terraform"
  }
}

# Staging layer: Cleaned and validated data
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "staging"
  friendly_name = "Staging Data"
  description   = "Cleaned, deduplicated, and validated data. Managed by dbt."
  location      = var.data_location
  project       = var.project_id

  labels = {
    environment = var.environment
    layer       = "staging"
    managed_by  = "terraform"
  }
}

# Reporting layer: Analytics-ready business models
resource "google_bigquery_dataset" "reporting" {
  dataset_id    = "reporting"
  friendly_name = "Reporting Data"
  description   = "Business-ready analytical models for reporting and analysis. Managed by dbt."
  location      = var.data_location
  project       = var.project_id

  labels = {
    environment = var.environment
    layer       = "reporting"
    managed_by  = "terraform"
  }
}

# -----------------------------------------------------------------------------
# Note: raw.fees table is created and managed by pipelines/ingest_fees.py
# Schema, partitioning, and lifecycle are defined in the Python ingestion script
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "raw_dataset_id" {
  description = "Raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "staging_dataset_id" {
  description = "Staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "reporting_dataset_id" {
  description = "Reporting dataset ID"
  value       = google_bigquery_dataset.reporting.dataset_id
}
