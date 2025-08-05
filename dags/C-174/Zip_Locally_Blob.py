import logging
from datetime import datetime
from io import BytesIO
import zipfile
from collections import defaultdict

from airflow import DAG
from airflow.decorators import task
from airflow.models.xcom_arg import XComArg

from azure.storage.blob import BlobServiceClient
from lib.operators.azure_connection_string import get_azure_connection_string

# Constants
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'
AZURE_CONTAINER_NAME = 'airflow'
SOURCE_PREFIX = 'C-194/archive_C-174/'
DESTINATION_PREFIX = 'C-194/restore_C-174/'

# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Zip_Azure_Blob_By_DateFolder_Expanded',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'C-194', 'AzureBlob', 'DateFolders', 'Expand'],
) as dag:

    @task
    def list_blobs_by_date_folder():
        logger = logging.getLogger("airflow.task")
        connection_string = get_azure_connection_string(AZURE_CONNECTION_NAME)
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

        blobs_by_date = defaultdict(list)
        total_blob_count = 0

        all_blobs = container_client.list_blobs(name_starts_with=SOURCE_PREFIX)
        for blob in all_blobs:
            if blob.name.lower().endswith('.zip') or blob.name.endswith('/'):
                continue
            relative_path = blob.name[len(SOURCE_PREFIX):]
            parts = relative_path.split('/', 1)
            if len(parts) == 2:
                date_folder, file_path = parts
                blobs_by_date[date_folder].append(blob.name)
                total_blob_count += 1

        logger.info("Total blobs to process: %d", total_blob_count)
        logger.info("Found %d date folders: %s", len(blobs_by_date), list(blobs_by_date.keys()))

        # Return list of dicts for dynamic task mapping
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

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for blob_name in blob_names:
                try:
                    blob_data = container_client.download_blob(blob_name).readall()
                    relative_path = blob_name[len(SOURCE_PREFIX):]
                    zipf.writestr(relative_path, blob_data)
                    logger.debug("Added %s to %s.zip", relative_path, date_folder)
                except Exception as e:
                    logger.error("Failed to add %s: %s", blob_name, str(e), exc_info=True)

        zip_buffer.seek(0)
        zip_blob_name = f"{DESTINATION_PREFIX}{date_folder}.zip"
        container_client.upload_blob(name=zip_blob_name, data=zip_buffer, overwrite=True)
        logger.info("Uploaded ZIP for folder %s: %s", date_folder, zip_blob_name)

    # DAG flow
    date_blob_groups = list_blobs_by_date_folder()
    zip_blobs_for_date_folder.expand(input=date_blob_groups)
