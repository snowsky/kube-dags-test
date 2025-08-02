import os
import re
import logging
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context

from hl7v2.msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback

# DAG config
default_args = {
    'owner': 'airflow',
}

MSH4_OID_CONFIG = 'hl7v2/msh4_oid.yaml'

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
    dag_id='Corepoint_to_SSI_OID_Extract',
    default_args=default_args,
    description='Parses HL7 files and resolves domain OID (MSH-4)',
    start_date=datetime(2025, 7, 10),
    catchup=False,
    max_active_runs=1,
    concurrency=100,
    tags=['C-179', 'hl7v2', 'Canary', 'Staging_in_Prod'],
    params={
        "max_workers": Param(50, type="integer", minimum=1),
        "batch_size": Param(500, type="integer", minimum=1)
    }
) as dag:

    @task
    def list_relevant_files() -> List[str]:
        base_dir = "/source-biakonzasftp/C-179/"
        target_pattern = "OB To SSI EUID"
        max_files = 2000000
        matched_files = []

        for root, dirs, files in os.walk(base_dir):
            if target_pattern in root:
                for file in files:
                    if file.endswith(".hl7") or file.endswith(".txt") or '.' not in file:
                        matched_files.append(os.path.join(root, file))
                        if len(matched_files) >= max_files:
                            logging.info(f"Reached file limit of {max_files}")
                            return matched_files
        logging.info(f"Total matched files: {len(matched_files)}")
        return matched_files

    @task
    def generate_batches(file_list: List[str]) -> List[List[str]]:
        context = get_current_context()
        batch_size = int(context["params"]["batch_size"])

        def divide_files_into_batches(file_list: List[str], batch_size: str) -> List[List[str]]:
            batch_size = int(batch_size)
            return [
                file_list[i: i + batch_size] 
                for i in range(0, len(file_list), batch_size)
            ]

        batches = divide_files_into_batches(file_list, batch_size)
        logging.info(f"Created {len(batches)} batches with batch size {batch_size}")
        return batches

    @task
    def process_batch(batch: List[str]):
        logging.info(f"Processing batch with {len(batch)} files")

        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        uploaded_items = []

        for file_path in batch:
            try:
                logging.info(f"Processing file: {file_path}")
                oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(file_path)
                if oid:
                    filename = os.path.basename(file_path)
                    s3_key = aws_key_pattern.format(OID=oid) + f"/{filename}"

                    key_exists = s3_hook.check_for_key(s3_key, bucket_name=bucket_name)

                    if not key_exists:
                        s3_hook.load_string(
                            string_data=oid,
                            key=s3_key,
                            bucket_name=bucket_name,
                            **s3_hook_kwargs
                        )
                        logging.info(f"Uploaded OID '{oid}' for file: {file_path} to s3://{bucket_name}/{s3_key}")
                        uploaded_items.append({"oid": oid, "file_path": file_path})

                        try:
                            os.remove(file_path)
                            logging.info(f"Deleted file after processing: {file_path}")
                        except Exception as delete_error:
                            logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
                else:
                    logging.warning(f"No OID extracted for: {file_path}")
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")

        logging.info(f"Batch uploaded items: {uploaded_items}")

    # DAG Chain
    all_files = list_relevant_files()
    batches = generate_batches(xml_files=all_files)
    process_batch.expand(batch=batches)
