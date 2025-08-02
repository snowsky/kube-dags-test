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

default_args = {
    'owner': 'airflow',
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
    def list_dated_folders_EUID6() -> str:
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID6"
        date_folder_pattern = re.compile(r"^\d{8}$")

        all_dated_folders = sorted(
            [
                os.path.join(base_dir, entry)
                for entry in os.listdir(base_dir)
                if os.path.isdir(os.path.join(base_dir, entry)) and date_folder_pattern.match(entry)
            ],
            reverse=True
        )

        if not all_dated_folders:
            raise FileNotFoundError("[EUID6] No valid dated folders found in directory.")

        selected_folder = all_dated_folders[-1]
        logging.info(f"[EUID6] Selected folder: {selected_folder}")
        return selected_folder

    @task
    def generate_batches(folder_path: str) -> List[List[str]]:
        context = get_current_context()
        batch_size = int(context["params"]["batch_size"])
        max_batches = 512
        max_files = batch_size * max_batches

        files = [
            os.path.join(folder_path, f)
            for f in sorted(os.listdir(folder_path))
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f
        ]

        files = files[:max_files]
        batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

        logging.info(f"[EUID6] Created {len(batches)} batches with batch size {batch_size}")
        return batches

    @task
    def process_all_batches(batches: List[List[str]]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        total_uploaded = 0

        for batch_index, batch in enumerate(batches):
            logging.info(f"[EUID6] Processing batch {batch_index + 1}/{len(batches)} with {len(batch)} files")
            for file_path in batch:
                try:
                    oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(file_path)
                    if oid:
                        filename = os.path.basename(file_path)
                        s3_key = aws_key_pattern.format(OID=oid) + f"/{filename}"

                        if not s3_hook.check_for_key(s3_key, bucket_name=bucket_name):
                            s3_hook.load_string(
                                string_data=oid,
                                key=s3_key,
                                bucket_name=bucket_name,
                                **s3_hook_kwargs
                            )
                            logging.info(f"[EUID6] Uploaded OID '{oid}' for file: {file_path}")
                            total_uploaded += 1

                            try:
                                os.remove(file_path)
                                logging.info(f"Deleted file after processing: {file_path}")
                            except Exception as delete_error:
                                logging.warning(f"Failed to delete: {file_path} | {delete_error}")
                        else:
                            logging.info(f"Key already exists: {s3_key}")
                    else:
                        logging.warning(f"No OID extracted for: {file_path}")
                except Exception as e:
                    logging.warning(f"Failed: {file_path} | {e}")

        logging.info(f"[EUID6] Total uploaded items: {total_uploaded}")

    # DAG Chain
    folder = list_dated_folders_EUID6()
    batches = generate_batches(folder)
    process_all_batches(batches)
