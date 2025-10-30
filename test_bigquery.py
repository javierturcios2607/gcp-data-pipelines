from google.cloud import bigquery

# ¡Magia! El cliente usa automáticamente las credenciales de la VM.
client = bigquery.Client()

# Tu consulta objetivo
query = """
    SELECT
      *
    FROM
      `riverajavier-dev.ML_Casas_Dataset.datos_entrenamiento_casas`
    LIMIT 1;
"""

print("🚀 Ejecutando consulta de prueba en BigQuery desde Python...")

try:
    query_job = client.query(query)
    results = query_job.result()

    print("✅ ¡ÉXITO! Resultados recibidos:")
    for row in results:
        print(f"--> Fila de datos encontrada. La primera columna es: {row[0]}")

    print("\n¡FELICIDADES! Has completado el ciclo. Tu estación de trabajo está 100% operativa.")

except Exception as e:
    print(f"❌ ERROR: La consulta falló. Causa: {e}")