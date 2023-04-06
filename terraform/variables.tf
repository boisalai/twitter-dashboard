locals {
  data_lake_bucket = "twitter_data_lake"
}

variable "project" {
  default = "dtc-de-382923"
  type = string
}

variable "region" {
  description = "Region for GCP resources"
  default = "northamerica-northeast1" 
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "twitter"
}

variable "instance" {
  type = string
  default = "twitter-vm"
}

variable "machine_type" {
  type = string
  default = "e2-standard-4"
}

variable "zone" {
  description = "Region for VM"
  type = string
  default = "northamerica-northeast1-a"
}
