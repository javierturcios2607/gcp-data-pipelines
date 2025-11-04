variable "project_id" {
  description = "El ID del proyecto de Google Cloud."
  type        = string  # SIN COMILLAS
  default     = "riverajavier-dev"
}

variable "region" {
  description = "La región principal para los recursos."
  type        = string  # SIN COMILLAS
  default     = "us-central1"
}

variable "bucket_name" {
  description = "El nombre único para el bucket del Data Lake."
  type        = string  # SIN COMillas
  default     = "data-lake-fenix-riverajavier"
}