output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "orders_topic" {
  value = google_pubsub_topic.orders.name
}

output "raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "marts_dataset" {
  value = google_bigquery_dataset.marts.dataset_id
}
