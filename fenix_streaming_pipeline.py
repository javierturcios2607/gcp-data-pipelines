import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from datetime import datetime, timezone

# Clase que define la lógica de transformación para cada mensaje
class ParseAndEnrich(beam.DoFn):
    def process(self, element):
        try:
            message_str = element.decode('utf-8')
            data = json.loads(message_str)

            if 'user_id' not in data or 'product_id' not in data:
                return # Ignora mensajes malformados

            # Añade la nueva columna con la hora actual en UTC
            data['processing_timestamp'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            yield data
        except Exception as e:
            print(f"Error al procesar el mensaje: {element}. Causa: {e}")

def run():
    # --- CONFIGURACIÓN DEL PIPELINE ---
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project='riverajavier-dev',
        region='us-central1',
        staging_location='gs://dataflow-staging-fenix-riverajavier-dev/staging',
        temp_location='gs://dataflow-staging-fenix-riverajavier-dev/temp',
        job_name='fenix-custom-streaming-pipeline',
        streaming=True
    )

    # --- DEFINICIÓN DEL PIPELINE ---
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription='projects/riverajavier-dev/subscriptions/clicks-stream-for-dataflow')
            | 'ParseAndEnrich' >> beam.ParDo(ParseAndEnrich())
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table='riverajavier-dev:fenix_dataset.clicks_enriched',
                schema='user_id:STRING,product_id:STRING,timestamp:TIMESTAMP,processing_timestamp:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == '__main__':
    run()