terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# Data Warehouse
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "my-dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

# Virtual Machine Instance
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_instance
# We need more than 10 GB, ideally 100 GB.
resource "google_compute_instance" "my-instance" {
  name         = var.instance
  machine_type = var.machine_type
  zone         = var.zone
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      size = 100
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network    = "default"
    subnetwork = "default"
    access_config {
      // Ephemeral IP
    }
  }
}
