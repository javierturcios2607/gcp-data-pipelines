resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # Permite borrar el bucket aunque tenga archivos

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 30 # Después de 30 días, mueve los datos a un almacenamiento más barato
    }
  }
}
