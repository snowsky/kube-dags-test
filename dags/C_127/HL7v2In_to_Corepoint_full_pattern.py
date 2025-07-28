from airflow import DAG
from airflow.models import Param
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from urllib.parse import unquote
import os
import re
import logging
import chardet

# Constants
DEFAULT_AZURE_CONTAINER = 'airflow'
DEFAULT_DEST_PATH_ARCHIVE = 'C-127/archive'
DEFAULT_DEST_PATH = 'C-179/HL7v2In_to_Corepoint_full'
AZURE_CONN_ID = 'biakonzasftp-blob-core-windows-net'
CHUNK_SIZE = 10000

class BucketDetails:
    def __init__(self, aws_conn_id, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.s3_hook_kwargs = s3_hook_kwargs

AWS_BUCKETS = {
    'konzaandssigrouppipelines': BucketDetails('konzaandssigrouppipelines', {}),
}

def check_and_rename_filename(file_key):
    decoded_key = unquote(file_key)
    file_name = decoded_key.split('/')[-1]
    pattern = r"HL7v2In/domainOid=[^/]+/root=([^/]+)/extension=([^/]+)/([0-9A-F\-]+)"
    match = re.search(pattern, decoded_key, re.IGNORECASE)
    if match:
        root, extension, uuid = match.groups()
        return f"root={root}_extension={extension}_{uuid}"
    return file_name if file_name else f"unnamed_{hash(file_key)}"

def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

@task
def list_s3_keys(aws_bucket: str, aws_folder: str) -> list:
    s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=aws_bucket, prefix=aws_folder)
    return [k for k in keys if not k.endswith('/')]

@task
def chunk_keys(keys: list) -> list:
    return list(chunk_list(keys, CHUNK_SIZE))

@task
def process_key_batch(file_keys: list, aws_bucket: str):
    s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
    azure_conn = BaseHook.get_connection(AZURE_CONN_ID)
    azure_connection_string = azure_conn.extra_dejson.get("connection_string")
    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

    for file_key in file_keys:
        try:
            file_name = check_and_rename_filename(file_key)
            current_date = datetime.now().strftime('%Y%m%d')
            dest1_path = os.path.join(DEFAULT_DEST_PATH, file_name)
            dest2_path = os.path.join(DEFAULT_DEST_PATH_ARCHIVE, current_date, file_name)

            file_obj = s3_hook.get_key(key=file_key, bucket_name=aws_bucket)
            raw_bytes = file_obj.get()["Body"].read()

            detected =
