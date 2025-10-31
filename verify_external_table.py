from google.cloud import bigquery

client = bigquery.Client()
query = """
    SELECT DISTINCT sale_date
    FROM `riverajavier-dev.fenix_dataset.sales_raw_external`
    ORDER BY sale_date;
"""

print("🔍 Verificando las particiones en la tabla externa...")
query_job = client.query(query)

dates_found = [row.sale_date for row in query_job.result()]

if len(dates_found) > 0:
    print(f"✅ ¡Éxito! Particiones encontradas para las fechas: {dates_found}")
else:
    print("❌ ERROR: No se encontraron particiones.")