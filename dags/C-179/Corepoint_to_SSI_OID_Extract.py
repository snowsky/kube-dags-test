import logging
import re
from datetime import datetime
from typing import List
from itertools import islice
from io import BytesIO

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context


from azure.storage.blob import BlobServiceClient
from hl7v2.msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback_from_bytes
from lib.operators.azure_connection_string import get_azure_connection_string

# Constants
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'
AZURE_CONNECTION_CONTAINER = 'airflow'
AZURE_CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)
MAX_FILES = 500_000

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 10),
}

class BucketDetails:
    def __init__(self, aws_conn_id, aws_key_pattern, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.aws_key_pattern = aws_key_pattern
        self.s3_hook_kwargs = s3_hook_kwargs

AWS_BUCKETS = {
    'com-ssigroup-insight-attribution-data': BucketDetails(
        aws_conn_id='konzaandssigrouppipelines',
        aws_key_pattern='subscriberName=KONZA/subscriptionName=HL7V2/source=HL7v2/status=pending/domainOid={OID}/{OID}',
        s3_hook_kwargs={'encrypt': True, 'acl_policy': 'bucket-owner-full-control'}
    )
}

with DAG(
    dag_id='Corepoint_to_SSI_OID_Extract_MountFree_InMemory',
    default_args=default_args,
    description='Parses HL7 files from Azure Blob in-memory and resolves domain OID (MSH-4)',
    catchup=False,
    max_active_runs=1,
    schedule_interval='@hourly',
    concurrency=50,
    tags=['C-179', 'hl7v2', 'Canary', 'Staging_in_Prod'],
    params={
        "batch_size": Param(10000, type="integer", minimum=1),
        "container_name": Param(AZURE_CONNECTION_CONTAINER, type="string"),
    }
) as dag:

    @task
    def list_relevant_blobs() -> List[str]:
        container_name = AZURE_CONNECTION_CONTAINER
        prefix = "C-179/OB To SSI EUID"
        pattern = re.compile(r"C-179/OB To SSI EUID\d+/")
    
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix)
    
        matched_blobs = []
    
        for i, blob in enumerate(islice(blob_list, MAX_FILES * 2)):
            # Log the first few blobs being scanned
            if i < 10:
                logging.info(f"Scanning blob: {blob.name}")
    
            if pattern.match(blob.name):
                matched_blobs.append(blob.name)
                logging.info(f"[{len(matched_blobs)}] Matched blob: {blob.name}")
                if len(matched_blobs) >= MAX_FILES:
                    break
    
            if len(matched_blobs) % 1000 == 0 and len(matched_blobs) > 0:
                logging.info(f"Matched {len(matched_blobs)} blobs so far (scanned {i + 1} blobs)...")
    
        logging.info(f"Finished listing. Total matched blobs: {len(matched_blobs)}")
        return matched_blobs
    


    @task
    def generate_batches(blob_paths: List[str]) -> List[List[str]]:
        context = get_current_context()
        batch_size = int(context["params"]["batch_size"])

        def divide_files_into_batches(xml_files: List[str], batch_size: str) -> List[List[str]]:
            batch_size = int(batch_size)
            return [
                xml_files[i: i + batch_size] 
                for i in range(0, len(xml_files), batch_size)
            ]

        batches = divide_files_into_batches(blob_paths, batch_size)
        logging.info(f"Created {len(batches)} batches with batch size {batch_size}")
        return batches

    @task
    def process_batch(batch: List[str]):
        logging.info(f"Processing batch with {len(batch)} blobs")
    
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONNECTION_CONTAINER)
    
        for blob_path in batch:
            try:
                logging.info(f"Processing blob: {blob_path}")
                blob_client = container_client.get_blob_client(blob_path)
                blob_data = blob_client.download_blob().readall()
    
                # Process HL7 content from memory
                oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback_from_bytes(blob_data)
                if oid:
                    filename = blob_path.split("/")[-1]
                    s3_key = aws_key_pattern.format(OID=oid) + f"/{filename}"
    
                    if not s3_hook.check_for_key(s3_key, bucket_name=bucket_name):
                        s3_hook.load_string(
                            string_data=oid,
                            key=s3_key,
                            bucket_name=bucket_name,
                            **s3_hook_kwargs
                        )
                        logging.info(f"Uploaded OID '{oid}' for blob: {blob_path} to s3://{bucket_name}/{s3_key}")
                        uploaded_items.append({"oid": oid, "blob_path": blob_path})
    
                        # Delete blob from Azure after successful upload
                        try:
                            blob_client.delete_blob()
                            logging.info(f"Deleted blob from Azure: {blob_path}")
                        except Exception as delete_error:
                            logging.warning(f"Failed to delete blob: {blob_path} | {delete_error}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
                else:
                    logging.warning(f"No OID extracted for: {blob_path}")
    
            except Exception as e:
                logging.warning(f"Failed to process blob: {blob_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")


    # DAG Chain
    all_blobs = list_relevant_blobs()
    batches = generate_batches(blob_paths=all_blobs)
    process_batch.expand(batch=batches)
