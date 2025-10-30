from google.cloud import bigquery

# --- CONFIGURACIÓN ---
# En un proyecto real, esto vendría de un archivo de configuración o variables de entorno
PROJECT_ID = "riverajavier-dev"
DATASET_ID = "fenix_dataset"
EXECUTION_DATE = "2025-10-30"
SQL_FILE = "load_optimized_sales.sql"
# --- FIN DE CONFIGURACIÓN ---

client = bigquery.Client(project=PROJECT_ID)

# Lee el contenido del archivo SQL
with open(SQL_FILE, 'r') as f:
    sql_template = f.read()

# Define los parámetros para la consulta
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("project_id", "STRING", PROJECT_ID),
        bigquery.ScalarQueryParameter("dataset_id", "STRING", DATASET_ID),
        bigquery.ScalarQueryParameter("execution_date", "DATE", EXECUTION_DATE),
    ]
)

print(f"🚀 Ejecutando el script SQL: {SQL_FILE}...")

# Ejecuta la consulta
query_job = client.query(sql_template, job_config=job_config)
query_job.result()  # Espera a que termine

print(f"✅ ¡Éxito! La tabla 'sales_optimized' ha sido cargada para la fecha {EXECUTION_DATE}.")