from airflow import DAG
from airflow.models import Param, Variable
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from urllib.parse import unquote
import os
import re
import logging

# Constants
DEFAULT_AZURE_CONTAINER = 'airflow'
DEFAULT_DEST_PATH_ARCHIVE = 'C-127/archive'
DEFAULT_DEST_PATH = 'C-179/HL7v2In_to_Corepoint_full'
AZURE_CONN_ID = 'azure_blob_conn'  # Airflow connection ID for Azure Blob Storage

class BucketDetails:
    def __init__(self, aws_conn_id, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.s3_hook_kwargs = s3_hook_kwargs

AWS_BUCKETS = {
    'konzaandssigrouppipelines': BucketDetails('konzaandssigrouppipelines', {}),
}

default_args = {'owner': 'airflow'}

def check_and_rename_filename(file_key):
    decoded_key = unquote(file_key)
    file_name = decoded_key.split('/')[-1]
    pattern = r"HL7v2In/domainOid=[^/]+/root=([^/]+)/extension=([^/]+)/([0-9A-F\-]+)"
    match = re.search(pattern, decoded_key, re.IGNORECASE)
    if match:
        root, extension, uuid = match.groups()
        return f"root={root}_extension={extension}_{uuid}"
    return file_name if file_name else f"unnamed_{hash(file_key)}"

def process_s3_to_azure(**kwargs):
    params = kwargs["params"]
    aws_bucket_name = params["aws_bucket"]
    aws_folder = params["aws_folder"]

    bucket_details = AWS_BUCKETS[aws_bucket_name]
    s3_hook = S3Hook(aws_conn_id=bucket_details.aws_conn_id)

    azure_conn = BaseHook.get_connection(AZURE_CONN_ID)
    azure_connection_string = azure_conn.extra_dejson.get("connection_string")
    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

    keys = s3_hook.list_keys(bucket_name=aws_bucket_name, prefix=aws_folder)
    for file_key in keys:
        if file_key.endswith('/'):
            continue

        file_name = check_and_rename_filename(file_key)
        current_date = datetime.now().strftime('%Y%m%d')
        dest1_path = os.path.join(DEFAULT_DEST_PATH, file_name)
        dest2_path = os.path.join(DEFAULT_DEST_PATH_ARCHIVE, current_date, file_name)

        file_content = s3_hook.read_key(key=file_key, bucket_name=aws_bucket_name)

        for dest_path in [dest1_path, dest2_path]:
            blob_client = blob_service_client.get_blob_client(container=DEFAULT_AZURE_CONTAINER, blob=dest_path)
            blob_client.upload_blob(file_content, overwrite=True)

        s3_hook.delete_objects(bucket=aws_bucket_name, keys=[file_key])

with DAG(
    dag_id='HL7v2In_to_Corepoint_full_pattern',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['C-127', 'C-179'],
    params={
        "aws_bucket": Param("konzaandssigrouppipelines", type="string"),
        "aws_folder": Param("HL7v2In/", type="string"),
    }
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_s3_to_azure',
        python_callable=process_s3_to_azure,
        provide_context=True
    )
