from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from lib.operators.azure_connection_string import get_azure_connection_string
import time

# Configuration
AZURE_CONNECTION_NAME = "biakonzasftp-blob-core-windows-net"
CONTAINER_NAME = "airflow"
CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)
SOURCE_PREFIX = "L-248/HL7v2_NJ/"
DESTINATION_PREFIX_TEMPLATE = "L-248/HL7v2_NJ_Original_{}/"
BATCH_SIZE = 2_000_000
MAX_BATCHES = 4  # For _Original_2 to _Original_5

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def move_blobs():
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    print("Listing blobs...")
    blob_list = list(container_client.list_blobs(name_starts_with=SOURCE_PREFIX))
    print(f"Total blobs found: {len(blob_list)}")

    for batch_index in range(MAX_BATCHES):
        start = batch_index * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_blobs = blob_list[start:end]
        destination_prefix = DESTINATION_PREFIX_TEMPLATE.format(batch_index + 2)

        print(f"\nProcessing batch {batch_index + 1}: {len(batch_blobs)} blobs to {destination_prefix}")

        for blob in batch_blobs:
            source_blob_name = blob.name
            destination_blob_name = source_blob_name.replace(SOURCE_PREFIX, destination_prefix, 1)

            source_blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{CONTAINER_NAME}/{source_blob_name}"
            destination_blob_client = container_client.get_blob_client(destination_blob_name)

            print(f"Copying {source_blob_name} to {destination_blob_name}...")
            destination_blob_client.start_copy_from_url(source_blob_url)

            while True:
                props = destination_blob_client.get_blob_properties()
                if props.copy.status != "pending":
                    break
                time.sleep(1)

            if props.copy.status == "success":
                print(f"Copy succeeded. Deleting original blob: {source_blob_name}")
                container_client.delete_blob(source_blob_name)
            else:
                print(f"Copy failed for {source_blob_name}. Status: {props.copy.status}")

with DAG(
    'azure_blob_batch_mover',
    default_args=default_args,
    description='Move Azure blobs in batches of 2 million and delete originals',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['azure', 'blob', 'batch'],
) as dag:

    move_blobs_task = PythonOperator(
        task_id='move_blobs_in_batches',
        python_callable=move_blobs
    )

    move_blobs_task
