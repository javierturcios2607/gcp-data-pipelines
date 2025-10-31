from google.cloud import bigquery

# --- CONFIGURACIÓN ---
PROJECT_ID = "riverajavier-dev"
DATASET_ID = "fenix_dataset"
TABLE_ID = "sales_raw_external"
BUCKET_NAME = "fenix-data-lake-dev-riverajavier-dev"
# --- FIN DE CONFIGURACIÓN ---

# Construye los identificadores
table_ref_str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
client = bigquery.Client(project=PROJECT_ID)

print(f"🚀 Iniciando la creación de la tabla externa: {table_ref_str}")
print("--> NOTA: Este script fallará si la tabla ya existe. Bórrala primero si es necesario.")

# Define el esquema de la tabla
schema = [
    bigquery.SchemaField("order_id", "INTEGER"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("amount", "FLOAT"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("sale_date", "DATE"),
]

# Define la configuración de la tabla externa
external_config = bigquery.ExternalConfig("CSV")
external_config.source_uris = [f"gs://{BUCKET_NAME}/sales/raw/*"]
external_config.skip_leading_rows = 1 # ¡LA LÍNEA MÁS IMPORTANTE!

# Configuración del particionamiento por Hive
hive_partitioning_opts = bigquery.HivePartitioningOptions()
hive_partitioning_opts.mode = "AUTO"
hive_partitioning_opts.source_uri_prefix = f"gs://{BUCKET_NAME}/sales/raw/"
external_config.hive_partitioning = hive_partitioning_opts

# Crea el objeto de la tabla
table = bigquery.Table(table_ref_str, schema=schema)
table.external_data_configuration = external_config

# Lanza la petición de creación a la API
created_table = client.create_table(table)

print(f"✅ ¡Éxito! La tabla externa '{created_table.table_id}' ha sido creada.")
