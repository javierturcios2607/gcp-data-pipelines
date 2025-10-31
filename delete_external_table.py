from google.cloud import bigquery

PROJECT_ID = "riverajavier-dev"
DATASET_ID = "fenix_dataset"
TABLE_ID = "sales_raw_external"

client = bigquery.Client(project=PROJECT_ID)
table_ref_str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

try:
    client.get_table(table_ref_str)
    print(f"🟡 Tabla '{table_ref_str}' encontrada. Borrándola ahora...")
    client.delete_table(table_ref_str)
    print(f"✅ ¡Éxito! La tabla ha sido borrada.")
except NotFound:
    print(f"🟢 La tabla '{table_ref_str}' no existe. No hay nada que borrar.")