import time
import json
import os
from google.cloud import pubsub_v1

# --- CONFIGURACIÓN ---
PROJECT_ID = "riverajavier-dev"
TOPIC_ID = "clicks-stream"
INPUT_FILE = "clicks_stream.jsonl"
# --- FIN DE CONFIGURACIÓN ---

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

print(f"🚀 Iniciando el productor de eventos para el tópico: {topic_path}")
print(f"--> Presiona Ctrl+C para detener el envío de mensajes.")

try:
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, INPUT_FILE)

    with open(file_path, 'r') as f:
        for line in f:
            message_data = line.strip()
            if not message_data:
                continue

            future = publisher.publish(topic_path, data=message_data.encode("utf-8"))
            print(f"✅ Mensaje enviado a Pub/Sub: {message_data} (ID: {future.result()})")
            time.sleep(2) 

    print("\n🏁 Fin del archivo. Todos los mensajes de muestra han sido enviados.")

except FileNotFoundError:
    print(f"❌ ERROR: No se encontró el archivo de entrada: {file_path}")
except KeyboardInterrupt:
    print("\n🛑 Productor detenido por el usuario.")
except Exception as e:
    print(f"❌ Ocurrió un error inesperado: {e}")