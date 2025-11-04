resource "google_bigquery_dataset" "fenix_dataset" {
  dataset_id = "fenix_dataset"
  location   = var.region
}

resource "google_bigquery_table" "sales_raw_external" {
  dataset_id = google_bigquery_dataset.fenix_dataset.dataset_id
  table_id   = "sales_raw_external"

  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/sales/raw/*"]


    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${google_storage_bucket.data_lake.name}/sales/raw/"
    }
  }

  # Leemos el esquema desde un archivo JSON separado para mantener el código limpio
  schema = file("${path.module}/schemas/sales_schema.json")
}