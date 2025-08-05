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
        total_blob_count =
