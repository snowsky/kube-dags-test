from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.email import send_email
import os
import pandas as pd
import sqlite3
import logging
from azure.storage.blob import BlobServiceClient
import glob
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define paths
base_path = '/source-biakonzasftp/S-6/SFTP/'
parquet_output_base = os.path.join(base_path, 'parquet')
archive_base = os.path.join(base_path, 'archive')

# Failure callback
def failure_callback(context):
    dag_name = context['dag'].dag_id
    dag_file_path = context['dag'].fileloc
    send_email(
        to='networksecurity@konza.org',
        subject=f'Task Failed in DAG: {dag_name}',
        html_content=f"Task {context['task_instance_key_str']} failed in DAG: {dag_name}. DAG source file: {dag_file_path}. Check the logs for more details."
    )

# Upload Parquet files to Azure Blob Storage
def upload_to_blob_storage(local_folder, partition_name):
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")

    container_name = 'content'
    blob_path_prefix = f'SFTP/{partition_name}/'

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    parquet_files = glob.glob(os.path.join(local_folder, '*.parquet'))
    for file_path in parquet_files:
        blob_name = blob_path_prefix + os.path.basename(file_path)
        with open(file_path, 'rb') as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
        logger.info(f'Uploaded {file_path} to blob {blob_name}')

# Main processing function
def process_sqlite_to_parquet():
    logger.info("Starting SQLite to Parquet conversion")

    for subdir in os.listdir(base_path):
        full_subdir_path = os.path.join(base_path, subdir)
        if os.path.isdir(full_subdir_path) and subdir.isdigit() and len(subdir) == 8:
            logger.info(f"Scanning subfolder: {full_subdir_path}")
            archive_path = os.path.join(archive_base, subdir)
            os.makedirs(archive_path, exist_ok=True)

            for entry in os.scandir(full_subdir_path):
                if entry.is_file() and '.' not in entry.name:
                    sqlite_path = entry.path
                    modified_time = os.path.getmtime(sqlite_path)
                    partition_name = datetime.fromtimestamp(modified_time).strftime('%Y-%m')
                    output_dir = os.path.join(parquet_output_base, partition_name)
                    os.makedirs(output_dir, exist_ok=True)

                    logger.info(f"Processing SQLite file: {sqlite_path}")
                    try:
                        conn = sqlite3.connect(sqlite_path)
                        cursor = conn.cursor()

                        # Get all table names
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = cursor.fetchall()

                        for table_name_tuple in tables:
                            table_name = table_name_tuple[0]
                            logger.info(f"Reading table: {table_name}")
                            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                            parquet_file_path = os.path.join(output_dir, f"{table_name}.parquet")
                            df.to_parquet(parquet_file_path)
                            logger.info(f"Saved table {table_name} to {parquet_file_path}")

                        conn.close()
                        logger.info(f"Finished processing {sqlite_path}")

                        # Upload to Azure Blob
                        upload_to_blob_storage(output_dir, partition_name)

                        # Move original file to archive/YYYYMMDD/
                        archived_file_path = os.path.join(archive_path, entry.name)
                        shutil.move(sqlite_path, archived_file_path)
                        logger.info(f"Moved original file to archive: {archived_file_path}")

                    except Exception as e:
                        logger.error(f"Failed to process {sqlite_path}: {e}")

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'on_failure_callback': failure_callback,
}

dag = DAG(
    'SFTP_sqllite_to_trino',
    default_args=default_args,
    description='Convert SQLite DBs from SFTP subfolders to Parquet, upload to Azure Blob, and archive originals',
    schedule_interval='@daily',
    catchup=False,
    tags=['S-6'],
)

process_sqlite_task = PythonOperator(
    task_id='process_sqlite_to_parquet',
    python_callable=process_sqlite_to_parquet,
    dag=dag,
    on_failure_callback=failure_callback,
)
