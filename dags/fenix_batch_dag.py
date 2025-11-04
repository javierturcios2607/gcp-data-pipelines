import pendulum
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

# --- ¡NUEVO! FUNCIÓN DE CALLBACK PARA ERRORES ---
# Airflow llamará a esta función si una tarea falla, pasándole un diccionario de contexto.
def on_task_failure(context):
    """
    Esta función se ejecuta cuando una tarea falla.
    Imprime un mensaje de alerta claro en los logs de Airflow.
    """
    task_instance = context['task_instance']
    print("="*50)
    print(f"🚨 ¡ALERTA! La tarea ha fallado. 🚨")
    print(f"   -> DAG: {task_instance.dag_id}")
    print(f"   -> Tarea: {task_instance.task_id}")
    print(f"   -> Fecha de Ejecución: {task_instance.execution_date}")
    print(f"   -> Log URL: {task_instance.log_url}")
    print("="*50)

# --- DEFINICIÓN DE CONSTANTES ---
PROJECT_ID = "riverajavier-dev"
DATASET_ID = "fenix_dataset"
GCS_BUCKET = "fenix-data-lake-dev-riverajavier-dev"

EXECUTION_DATE_MACRO = "{{ ds }}"

LOAD_OPTIMIZED_TABLE_SQL = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.sales_optimized`
    PARTITION BY sale_date
    CLUSTER BY product_id
    AS
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.sales_raw_external`
    WHERE sale_date = @execution_date;
"""

# --- DEFINICIÓN DEL DAG ---
with DAG(
    dag_id="fenix_daily_batch_load",
    start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
    schedule_interval="0 3 * * *",
    catchup=False,
    # ¡NUEVO! Podemos definir callbacks a nivel de DAG, que se aplicarán a todas las tareas.
    on_failure_callback=on_task_failure,
    tags=["fenix_project", "batch", "data_engineering"],
) as dag:

    # --- TAREA #1: EL SENSOR (EL GUARDIA) ---
    wait_for_sales_file = GCSObjectExistenceSensor(
        task_id="wait_for_sales_file",
        bucket=GCS_BUCKET,
        object=f"sales/raw/dt={EXECUTION_DATE_MACRO}/_SUCCESS",
        mode="poke",
        poke_interval=60, # Reducido a 60s para pruebas más rápidas
        timeout=60 * 30, # Timeout de 30 mins
    )

    # --- TAREA #2: LA CARGA A BIGQUERY (EL MÚSICO) ---
    load_to_optimized_table = BigQueryInsertJobOperator(
        task_id="load_sales_optimized_from_external",
        configuration={
            "query": {
                "query": LOAD_OPTIMIZED_TABLE_SQL,
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "execution_date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {"value": EXECUTION_DATE_MACRO},
                    }
                ],
            }
        },
        location="US",
    )

    # --- DEFINICIÓN DE DEPENDENCIAS ---
    wait_for_sales_file >> load_to_optimized_table
