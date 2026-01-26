variable "project" {
  description = "GCP Project"
  default     = "de-zoomcamp-397401"
}

variable "region" {
  description = "GCP Region"
  default     = "us-east1"
}

variable "credentials" {
  description = "My credentials"
  default     = "./gcp-key.json"
}

variable "location" {
  description = "GCP Location"
  default     = "US"
}

variable "bq_datasetname" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset_397401"
}

variable "gcs_bucket_name" {
  description = "My GCS Bucket Name"
  default     = "demo-bucket-397401"
}


variable "gcs_storage_class" {
  description = "Byucket Storage Class"
  default     = "STANDARD"
}