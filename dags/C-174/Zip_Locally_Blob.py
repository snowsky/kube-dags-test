import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import zipfile
from collections import defaultdict

from airflow import DAG
from airflow.operators.python import PythonOperator

from azure.storage.blob import BlobServiceClient
from lib.operators.azure_connection_string import get_azure_connection_string

# Constants
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'
AZURE_CONTAINER_NAME = 'airflow'
SOURCE_PREFIX = 'C-194/archive_C-174/'
DESTINATION_PREFIX = 'C-194/restore_C-174/'

def batch_zip_by_date_folder():
    logger = logging.getLogger("airflow.task")

    connection_string = get_azure_connection_string(AZURE_CONNECTION_NAME)
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

    # Group blobs by date folder
    blobs_by_date = defaultdict(list)
    all_blobs = container_client.list_blobs(name_starts_with=SOURCE_PREFIX)

    for blob in all_blobs:
        if blob.name.lower().endswith('.zip') or blob.name.endswith('/'):
            continue
        relative_path = blob.name[len(SOURCE_PREFIX):]
        parts = relative_path.split('/', 1)
        if len(parts) == 2:
            date_folder, file_path = parts
            blobs_by_date[date_folder].append(blob.name)

    logger.info("Found %d date folders", len(blobs_by_date))

    def zip_date_folder(date_folder, blob_names):
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for blob_name in blob_names:
                try:
                    blob_data = container_client.download_blob(blob_name).readall()
                    relative_path = blob_name[len(SOURCE_PREFIX):]
                    zipf.writestr(relative_path, blob_data)
                    logger.info("Added %s to %s.zip", relative_path, date_folder)
                except Exception as e:
                    logger.error("Failed to add %s: %s", blob_name, str(e), exc_info=True)

        zip_buffer.seek(0)
        zip_blob_name = f"{DESTINATION_PREFIX}{date_folder}.zip"
        container_client.upload_blob(name=zip_blob_name, data=zip_buffer, overwrite=True)
        logger.info("Uploaded %s", zip_blob_name)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for date_folder, blob_names in blobs_by_date.items():
            executor.submit(zip_date_folder, date_folder, blob_names)

# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Zip_Azure_Blob_By_Date_Folder',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'C-194', 'AzureBlob', 'DateFolders'],
) as dag:

    zip_task = PythonOperator(
        task_id='batch_zip_by_date_folder',
        python_callable=batch_zip_by_date_folder,
    )
