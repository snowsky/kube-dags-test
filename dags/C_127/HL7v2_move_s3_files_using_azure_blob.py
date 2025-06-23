import os
import logging
from io import BytesIO
from datetime import datetime
from typing import List
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from azure.storage.blob import BlobServiceClient
import paramiko
import json

# DAG Config
#ENV = 'Prod'
BUCKET_NAME = 'konzaandssigrouppipelines'
S3_SUBFOLDER = 'HL7v2In/'
#S3_SUBFOLDER = 'HL7v2In/aa_test/'

# 1st Location 
AZURE_CONTAINER = 'airflow'
AZURE_DEST_DIR = 'C-179/HL7v2In_to_Corepoint'
AZURE_CONN_ID = 'biakonzasftp-blob-core-windows-net'

#2nd Location

BIA_DEST_DIR = '/source-biakonzasftp/C-127/archive'
#BIA_DEST_DIR = '/data/biakonzasftp/C-127/archive'



default_args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='HL7v2_move_s3_files_using_azure_blob',
    default_args=default_args,
    description='Move HL7v2 files from S3 to Azure with root/extension-based renaming',
   #schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=50,
    tags=['C-127', 'Canary', 'Staging_in_Prod'],
    params={
        "max_workers": Param(50, type="integer", minimum=1),
        "batch_size": Param(500, type="integer", minimum=1) #  - Warning do not set higher than 500 (testing on 2/7 around duration greater than 1 hour hitting timeout defined below)
    }
)

def get_azure_connection_string(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)
    return conn.extra_dejson.get("connection_string")

AZURE_CONNECTION_STRING = get_azure_connection_string(AZURE_CONN_ID)

@task(dag=dag)
def list_files_in_s3() -> List[str]:
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    files = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_SUBFOLDER) or []
    logging.info(f"Found {len(files)} files in S3")
    return [f for f in files if not f.endswith('/')]

@task(dag=dag)
def divide_into_batches(files: List[str], batch_size: str) -> List[List[str]]:
    batch_size = int(batch_size)
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

def extract_root_and_extension(key: str):
    parts = key.split('/')
    root = next((p.split('=')[1] for p in parts if p.startswith('root=')), None)
    ext = next((p.split('=')[1] for p in parts if p.startswith('extension=')), None)
    return root, ext

def rename_file(root: str, ext: str, original: str) -> str:
    return f"root={root}_extension={ext}_{original}.txt"

def upload_to_azure(blob_path: str, content: BytesIO):
    try:
        blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        blob_client = blob_service.get_blob_client(container=AZURE_CONTAINER, blob=blob_path)
        blob_client.upload_blob(content, overwrite=True)
        logging.info(f"Uploaded to Azure: {blob_path}")
    except Exception as e:
        logging.error(f"Failed to upload to Azure blob at {blob_path}: {e}")
        raise

## For BIA 
def upload_to_bia_dest(local_path: str, content: BytesIO):
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            f.write(content.read())
        logging.info(f"File written to: {local_path}")
    except Exception as e:
        logging.error(f"Failed to write to {local_path}: {e}")
        raise

def delete_from_s3(key: str):
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    s3_hook.delete_objects(bucket=BUCKET_NAME, keys=[key])
    logging.info(f"Deleted from S3: {key}")

@task(dag=dag)
def process_batch_airflow_container(batch: List[str]):
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    #today = datetime.today().strftime('%Y%m%d')

    for key in batch:
        try:
            root, ext = extract_root_and_extension(key)
            original_filename = key.split('/')[-1]

            # Logging extracted parts
            logging.info(f"Processing file: {key}")
            logging.info(f"Extracted root: {root}")
            logging.info(f"Extracted extension: {ext}")
            logging.info(f"Original filename: {original_filename}")

            if not root or not ext:
                logging.warning(f"Skipping {key}: missing root or extension")
                continue

            new_filename = rename_file(root, ext, original_filename)
            azure_blob_path = f"{AZURE_DEST_DIR}/{new_filename}"
            

            file_stream = BytesIO()
            s3_obj = s3_hook.get_key(key=key, bucket_name=BUCKET_NAME)
            s3_obj.download_fileobj(file_stream)
            file_stream.seek(0)

            upload_to_azure(azure_blob_path, file_stream)

        except Exception as e:
            logging.error(f"Failed to process {key}: {e}")
            raise

@task(dag=dag)
def process_batch_biakonzasftp_container(batch: List[str]):
    s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    today = datetime.today().strftime('%Y%m%d')

    for key in batch:
        try:
            root, ext = extract_root_and_extension(key)
            original_filename = key.split('/')[-1]

            # Logging extracted parts
            logging.info(f"Processing file: {key}")
            logging.info(f"Extracted root: {root}")
            logging.info(f"Extracted extension: {ext}")
            logging.info(f"Original filename: {original_filename}")

            if not root or not ext:
                logging.warning(f"Skipping {key}: missing root or extension")
                continue

            new_filename = rename_file(root, ext, original_filename)
            bia_dest_path = f"{BIA_DEST_DIR}/{today}/{new_filename}"
            

            file_stream = BytesIO()
            s3_obj = s3_hook.get_key(key=key, bucket_name=BUCKET_NAME)
            s3_obj.download_fileobj(file_stream)
            file_stream.seek(0)

            upload_to_bia_dest(bia_dest_path, file_stream)

        except Exception as e:
            logging.error(f"Failed to process {key}: {e}")
            raise

# Workflow chaining
all_files = list_files_in_s3()
batches = divide_into_batches(all_files, batch_size="{{ params.batch_size }}")
transfer_tasks_airflow_container = process_batch_airflow_container.expand(batch=batches)
transfer_tasks_biakonzasftp_container = process_batch_biakonzasftp_container.expand(batch=batches)


all_files >> batches >> transfer_tasks_airflow_container >> transfer_tasks_biakonzasftp_container

if __name__ == "__main__":
    dag.cli()
