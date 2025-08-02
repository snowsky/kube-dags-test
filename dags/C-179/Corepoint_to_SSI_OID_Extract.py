import logging
import re
from datetime import datetime
from typing import List
from itertools import islice

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context

from azure.storage.blob import BlobServiceClient
from hl7v2.msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback
from lib.operators.azure_connection_string import get_azure_connection_string

# Constants
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'
AZURE_CONNECTION_CONTAINER = 'airflow'
AZURE_CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)
MAX_FILES = 1_000_000

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
    dag_id='Corepoint_to_SSI_OID_Extract_MountFree',
    default_args=default_args,
    description='Parses HL7 files from Azure Blob and resolves domain OID (MSH-4)',
    catchup=False,
    max_active_runs=1,
    concurrency=100,
    tags=['C-179', 'hl7v2', 'Canary', 'Staging_in_Prod'],
    params={
        "batch_size": Param(500, type="integer", minimum=1),
        "container_name": Param(AZURE_CONNECTION_CONTAINER, type="string"),
    }
) as dag:

    @task
    def list_relevant_blobs() -> List[str]:
        container_name = AZURE_CONNECTION_CONTAINER
        prefix = "C-179/"
        pattern = re.compile(r"C-179/OB To SSI EUID\d+/")

        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)

        blob_list = container_client.list_blobs(name_starts_with=prefix)
        matched_blobs = []

        for blob in islice(blob_list, MAX_FILES * 2):  # Overfetch to allow filtering
            if pattern.match(blob.name):
                matched_blobs.append(blob.name)
                if len(matched_blobs) >= MAX_FILES:
                    break

        logging.info(f"Found {len(matched_blobs)} matching blobs.")
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

        for blob_path in batch:
            try:
                logging.info(f"Processing blob: {blob_path}")
                # Download blob content
                blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
                container_client = blob_service_client.get_container_client(AZURE_CONNECTION_CONTAINER)
                blob_client = container_client.get_blob_client(blob_path)
                blob_data = blob_client.download_blob().readall()

                # Save to temp file
                temp_file_path = f"/tmp/{blob_path.replace('/', '_')}"
                with open(temp_file_path, "wb") as f:
                    f.write(blob_data)

                oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(temp_file_path)
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
                    else:
                        logging.info(f"Key already exists: {s3_key}")
                else:
                    logging.warning(f"No OID extracted for: {blob_path}")

                # Clean up
                try:
                    import os
                    os.remove(temp_file_path)
                except Exception as e:
                    logging.warning(f"Failed to delete temp file: {temp_file_path} | {e}")

            except Exception as e:
                logging.warning(f"Failed to process blob: {blob_path} | {e}")

        logging.info(f"Batch uploaded items: {uploaded_items}")

    # DAG Chain
    all_blobs = list_relevant_blobs()
    batches = generate_batches(blob_paths=all_blobs)
    process_batch.expand(batch=batches)
