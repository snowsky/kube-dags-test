import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from lib.operators.azure_connection_string import get_azure_connection_string

# Configuration
AZURE_CONNECTION_NAME = "biakonzasftp-blob-core-windows-net"
CONTAINER_NAME = "airflow"
CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)
SOURCE_PREFIX = "L-248/HL7v2_NJ/"
DESTINATION_PREFIX_TEMPLATE = "L-248/HL7v2_NJ_Original_{}/"
BATCH_SIZE = 2_000_000
MAX_BATCHES = 4

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def move_blobs():
    logging.info("Starting blob move process...")
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    blob_iterator = container_client.list_blobs(name_starts_with=SOURCE_PREFIX)

    batch = []
    batch_index = 0
    total_moved = 0

    for blob in blob_iterator:
        batch.append(blob)
        if len(batch) >= BATCH_SIZE:
            if batch_index >= MAX_BATCHES:
                break
            destination_prefix = DESTINATION_PREFIX_TEMPLATE.format(batch_index + 2)
            logging.info(f"Processing batch {batch_index + 1} with {len(batch)} blobs to {destination_prefix}")
            total_moved += process_batch(container_client, blob_service_client, batch, destination_prefix)
            batch = []
            batch_index += 1

    if batch and batch_index < MAX_BATCHES:
        destination_prefix = DESTINATION_PREFIX_TEMPLATE.format(batch_index + 2)
        logging.info(f"Processing final batch {batch_index + 1} with {len(batch)} blobs to {destination_prefix}")
        total_moved += process_batch(container_client, blob_service_client, batch, destination_prefix)

    logging.info(f"Blob move process completed. Total blobs moved: {total_moved}")

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
    'azure_blob_batch_mover_streamed',
    default_args=default_args,
    description='Stream and move Azure blobs in batches of 2 million',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['L-248'],
) as dag:

    move_blobs_task = PythonOperator(
        task_id='move_blobs_in_batches',
        python_callable=move_blobs
    )

    move_blobs_task
