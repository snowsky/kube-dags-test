import logging
from datetime import datetime
from io import BytesIO
import zipfile
from collections import defaultdict
import time
import json

from airflow import DAG
from airflow.decorators import task
from airflow.models.xcom_arg import XComArg

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ServiceRequestError
from lib.operators.azure_connection_string import get_azure_connection_string

# Constants
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'
AZURE_CONTAINER_NAME = 'airflow'
SOURCE_PREFIX = 'C-194/archive_C-174/'
DESTINATION_PREFIX = 'C-194/restore_C-174/'
CHECKPOINT_BLOB = f"{DESTINATION_PREFIX}checkpoints/checkpoints.json"
MAX_BLOBS = 20_000_000

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 16),
    'retries': 1,
}

def safe_list_blobs(container_client, prefix, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return container_client.list_blobs(name_starts_with=prefix, results_per_page=500)
        except ServiceRequestError as e:
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                raise e

def read_checkpoint(container_client):
    try:
        blob_client = container_client.get_blob_client(CHECKPOINT_BLOB)
        data = blob_client.download_blob().readall()
        return set(json.loads(data))
    except Exception:
        return set()

def write_checkpoint(container_client, processed_folders):
    blob_client = container_client.get_blob_client(CHECKPOINT_BLOB)
    blob_client.upload_blob(json.dumps(list(processed_folders)), overwrite=True)

with DAG(
    dag_id='Zip_Azure_Blob_By_DateFolder_Expanded',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,  # Manual trigger only
    tags=['C-174', 'C-194', 'AzureBlob', 'DateFolders', 'Expand'],
) as dag:

    @task
    def list_blobs_by_date_folder():
        logger = logging.getLogger("airflow.task")
        connection_string = get_azure_connection_string(AZURE_CONNECTION_NAME)
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

        processed_folders = read_checkpoint(container_client)
        blobs_by_date = defaultdict(list)
        total_blob_count = 0

        all_blobs = safe_list_blobs(container_client, SOURCE_PREFIX)
        paged_blobs = all_blobs.by_page()

        for page in paged_blobs:
            for blob in page:
                if total_blob_count >= MAX_BLOBS:
                    break
                if blob.name.lower().endswith('.zip') or blob.name.endswith('/'):
                    continue
                relative_path = blob.name[len(SOURCE_PREFIX):]
                parts = relative_path.split('/', 1)
                if len(parts) == 2:
                    date_folder, file_path = parts
                    if date_folder not in processed_folders:
                        blobs_by_date[date_folder].append(blob.name)
                        total_blob_count += 1
                        if total_blob_count % 10000 == 0:
                            logger.info("Processed %d blobs so far...", total_blob_count)
            if total_blob_count >= MAX_BLOBS:
                break

        logger.info("Total blobs to process: %d", total_blob_count)
        return [{'date_folder': k, 'blob_names': v} for k, v in blobs_by_date.items()]

    @task
    def zip_blobs_for_date_folder(input: dict):
        logger = logging.getLogger("airflow.task")
        date_folder = input['date_folder']
        blob_names = input['blob_names']

        logger.info("Processing folder: %s with %d blobs", date_folder, len(blob_names))

        connection_string = get_azure_connection_string(AZURE_CONNECTION_NAME)
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

        start_time = time.time()
        zip_buffer = BytesIO()
        blob_count = 0

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for blob_name in blob_names:
                try:
                    blob_data = container_client.download_blob(blob_name).readall()
                    relative_path = blob_name[len(SOURCE_PREFIX):]
                    zipf.writestr(relative_path, blob_data)
                    blob_count += 1
                except Exception as e:
                    logger.warning("Failed to add %s: %s", blob_name, str(e))

        zip_buffer.seek(0)
        zip_blob_name = f"{DESTINATION_PREFIX}{date_folder}.zip"
        container_client.upload_blob(name=zip_blob_name, data=zip_buffer, overwrite=True)

        duration = time.time() - start_time
        zip_size = zip_buffer.getbuffer().nbytes

        logger.info("Uploaded ZIP for folder %s: %s", date_folder, zip_blob_name)
        logger.info("Metrics - Blobs: %d, Size: %.2f MB, Duration: %.2f sec",
                    blob_count, zip_size / (1024 * 1024), duration)

        # Update checkpoint
        processed_folders = read_checkpoint(container_client)
        processed_folders.add(date_folder)
        write_checkpoint(container_client, processed_folders)

    date_blob_groups = list_blobs_by_date_folder()
    zip_blobs_for_date_folder.expand(input=date_blob_groups)
