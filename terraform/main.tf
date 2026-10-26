terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Raw + curated buckets. Small simulated volumes: standard storage is fine.
resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-retail-raw"
  location      = var.region
  force_destroy = true
}

resource "google_storage_bucket" "curated" {
  name          = "${var.project_id}-retail-curated"
  location      = var.region
  force_destroy = true
}

# Streaming topics. The bridge forwards Kafka events here.
resource "google_pubsub_topic" "orders" {
  name = "retail-orders"
}

resource "google_pubsub_topic" "clicks" {
  name = "retail-clicks"
}

resource "google_pubsub_topic" "inventory" {
  name = "retail-inventory"
}

# Warehouse datasets.
resource "google_bigquery_dataset" "raw" {
  dataset_id = "retail_raw"
  location   = var.region
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = "retail_marts"
  location   = var.region
}

# Raw events table: partitioned by event date, clustered by event type,
# so queries scan only the partitions they need.
resource "google_bigquery_table" "events" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "events"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_ts"
  }
  clustering = ["event_type"]

  schema = <<EOF
[
  {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
  {"name": "event_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "amount", "type": "FLOAT64", "mode": "NULLABLE"},
  {"name": "quantity", "type": "INT64", "mode": "NULLABLE"}
]
EOF
}

# NOTE: Cloud Composer is intentionally NOT created here. It is the most
# expensive piece of this project. Create it only when testing the DAG in
# batch/dags/, and delete it right after.
