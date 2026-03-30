variable "project" {
  description = "Your GCP Project ID"
  default     = "your-project-id" # Change to your GCP Project ID
}

variable "credentials" {
  description = "The key of your GCP service account in JSON format"
  default     = "../.credentials/gcp_key.json" 
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project location "
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket name to save raw data extracted from API"
  default     = "your-bucket-name" # Change to your GCP bucket name
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "bq_raw_dataset" {
  description = "BigQuery dataset for raw data"
  default     = "raw_dataset"
}

variable "bq_prod_dataset" {
  description = "BigQuery dataset for production data after transformation in dbt"
  default     = "prod_dataset"
}