# -----------------------------------------------------------------------------
# Google Cloud Storage - Data Lake
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  project       = var.project_id
  force_destroy = true # Set to false in production

  # Uniform bucket-level access (recommended)
  uniform_bucket_level_access = true

  # Versioning for audit trail
  versioning {
    enabled = true
  }

  # Lifecycle rules for cost optimization
  lifecycle_rule {
    condition {
      age = 90 # Days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365 # Days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = {
    environment = var.environment
    purpose     = "data-lake"
    managed_by  = "terraform"
  }
}

# -----------------------------------------------------------------------------
# Folder structure (using empty objects as folder markers)
# -----------------------------------------------------------------------------

resource "google_storage_bucket_object" "raw_fees_folder" {
  name    = "raw/fees/"
  bucket  = google_storage_bucket.data_lake.name
  content = " " # Empty content, just creates the "folder"
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "data_lake_bucket" {
  description = "Data lake GCS bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "data_lake_uri" {
  description = "Data lake GCS URI"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "raw_fees_path" {
  description = "GCS path for raw fees data"
  value       = "gs://${google_storage_bucket.data_lake.name}/raw/fees/"
}
