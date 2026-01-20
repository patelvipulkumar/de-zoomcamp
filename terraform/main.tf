terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region

}

resource "google_storage_bucket" "dtc-de-gcb" {
  name                        = var.gcs_bucket_name
  location                    = var.location
  storage_class               = var.gcs_storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true

}

resource "google_bigquery_dataset" "dtc-de-bq" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}



