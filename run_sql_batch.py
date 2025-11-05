from google.cloud import bigquery
import os

# --- CONFIGURACIÓN ---
PROJECT_ID = "riverajavier-dev"
DATASET_ID = "fenix_dataset"
EXECUTION_DATE = "2025-11-06"
SQL_FILE = "load_optimized_sales.sql"
# --- FIN DE CONFIGURACIÓN ---

client = bigquery.Client(project=PROJECT_ID)

# --- Construcción de la Consulta ---
script_dir = os.path.dirname(__file__)
file_path = os.path.join(script_dir, SQL_FILE)

try:
    with open(file_path, 'r') as f:
        sql_template = f.read()
except FileNotFoundError:
    print(f"❌ ERROR: No se encontró el archivo SQL: {file_path}")
    exit()

# ¡LA MAGIA! Usamos .format() de Python para rellenar las llaves {} en el SQL.
sql_query_final = sql_template.format(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID
)

# --- Configuración de Parámetros de VALOR ---
# El único parámetro de valor que queda es @execution_date
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("execution_date", "DATE", EXECUTION_DATE),
    ]
)

print(f"🚀 Ejecutando la consulta para la fecha {EXECUTION_DATE}...")

# --- Ejecución ---
try:
    # Le pasamos la consulta ya formateada a BigQuery
    query_job = client.query(sql_query_final, job_config=job_config)
    query_job.result()  # Espera a que termine

    print(f"✅ ¡Éxito! La tabla 'sales_optimized' ha sido cargada/actualizada.")

except Exception as e:
    print(f"❌ ERROR durante la ejecución de la consulta en BigQuery: {e}")