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
