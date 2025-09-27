import logging
import time
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from lib.operators.azure_connection_string import get_azure_connection_string

# Configuration
AZURE_CONNECTION_NAME = "biakonzasftp-blob-core-windows-net"
CONTAINER_NAME = "airflow"
CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)
SOURCE_PREFIX = "L-248/HL7v2_NJ/"
DESTINATION_PREFIX_TEMPLATE = "L-248/HL7v2_NJ_Original_{}/"
MAX_BATCHES = 49
BATCH_SIZE = 40_000_000 // MAX_BATCHES  # ≈ 816,327

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def move_batch(batch_index):
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.info(f"Starting batch {batch_index + 1}...")

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    blob_iterator = container_client.list_blobs(name_starts_with=SOURCE_PREFIX)

    start_index = batch_index * BATCH_SIZE
    end_index = start_index + BATCH_SIZE
    batch_blobs = []

    for i, blob in enumerate(blob_iterator):
        if i >= end_index:
            break
        if i >= start_index:
            batch_blobs.append(blob)

    destination_prefix = DESTINATION_PREFIX_TEMPLATE.format(batch_index + 2)
    logging.info(f"Processing batch {batch_index + 1} with {len(batch_blobs)} blobs to {destination_prefix}")
    total_moved = process_batch(container_client, blob_service_client, batch_blobs, destination_prefix)
    logging.info(f"Completed batch {batch_index + 1}. Total blobs moved: {total_moved}")

def process_batch(container_client, blob_service_client, batch_blobs, destination_prefix):
    successful_copies = 0
    for blob in batch_blobs:
        source_blob_name = blob.name
        destination_blob_name = source_blob_name.replace(SOURCE_PREFIX, destination_prefix, 1)
        source_blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{CONTAINER_NAME}/{source_blob_name}"
        destination_blob_client = container_client.get_blob_client(destination_blob_name)

        try:
            destination_blob_client.start_copy_from_url(source_blob_url)
            while True:
                props = destination_blob_client.get_blob_properties()
                if props.copy.status != "pending":
                    break
                time.sleep(1)

            if props.copy.status == "success":
                container_client.delete_blob(source_blob_name)
                successful_copies += 1
                logging.info(f"Copied and deleted: {source_blob_name} → {destination_blob_name}")
            else:
                logging.warning(f"Copy failed for {source_blob_name}. Status: {props.copy.status}")
        except Exception as e:
            logging.error(f"Error processing {source_blob_name}: {e}")
    return successful_copies

with DAG(
    'azure_blob_batch_mover_parallel',
    default_args=default_args,
    description='Move 40M Azure blobs in 49 parallel batches',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['L-248'],
    max_active_tasks=50,  # Optional: limit concurrency
) as dag:

    for batch_index in range(MAX_BATCHES):
        PythonOperator(
            task_id=f'move_batch_{batch_index + 2}',
            python_callable=move_batch,
            op_args=[batch_index],
        )
