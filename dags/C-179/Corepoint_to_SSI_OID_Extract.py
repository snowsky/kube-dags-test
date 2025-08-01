import re
import yaml
from typing import Dict, List, Union
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context

from hl7v2.msh4_oid import get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback

import logging
# For Prod
import os
import hl7

# DAG definition

MSH4_OID_CONFIG = 'hl7v2/msh4_oid.yaml'

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
    #schedule_interval='@hourly',
    tags=['C-179','hl7v2', 'Canary', 'Staging_in_Prod'],
    params={
        "max_workers": Param(50, type="integer", minimum=1),
        "batch_size": Param(500, type="integer", minimum=1)
    }
) as dag:

    @task(dag=dag)
    def list_dated_folders_EUID6() -> List[str]:
        base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID6"
        #base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID6"
        date_folder_pattern = re.compile(r"^\d{8}$")
    
        # Get all valid dated folders and sort newest to oldest
        all_dated_folders = sorted(
            [
                os.path.join(base_dir, entry)
                for entry in os.listdir(base_dir)
                if os.path.isdir(os.path.join(base_dir, entry)) and date_folder_pattern.match(entry)
            ],
            reverse=True
        )
    
        logging.info(f"[EUID6] All dated folders (newest to oldest): {all_dated_folders}")
    
        if not all_dated_folders:
            raise FileNotFoundError("[EUID6] No valid dated folders found in directory.")
    
        selected_folder = all_dated_folders[-1]
        logging.info(f"[EUID6] Selected smallest (oldest) folder to process: {selected_folder}")
        return [selected_folder]

    @task(dag=dag)
    def chunk_files_in_folder(folder_path: str) -> List[List[str]]:
        context = get_current_context()
        batch_size = int(context["params"]["batch_size"])
        max_mapped_tasks = 512
    
        # Limit to first 10 files for testing
        files = []
        #for f in sorted(os.listdir(folder_path))[:5]:
        for f in sorted(os.listdir(folder_path)):
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f:
                files.append(os.path.join(folder_path, f))
    
        if not files:
            logging.warning(f"[EUID6] No eligible files in: {folder_path}")
            return []
    
        folder_batch_size = (
            (len(files) // max_mapped_tasks) + 1 if len(files) > max_mapped_tasks * batch_size else batch_size
        )
    
        batches = [files[i:i + folder_batch_size] for i in range(0, len(files), folder_batch_size)]

        #logging.info(
            #f"[EUID6] Folder: {folder_path} | Files: {len(files)} | Files per batch: {folder_batch_size} | Batches: {len(batches)}"
        #)
        #logging.info(f"Returning file batches: {batches}")
        #logging.info(f"Returning file batches: {batches} | Type: {type(batches)}")
        
        return batches
        #return batches[0] if batches else []


                
    @task(dag=dag)
    def extract_and_upload_EUID6(file_batch: List[str]):
        logging.info(f"[EUID6] Received file_batch: {file_batch}")  # ← Add this here ✅

        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
        file_batch = file_batch[0]
        for file_path in file_batch:
            try:
                logging.info(f"[EUID6] Processing file: {file_path}")
                oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(file_path)
                if oid:
                    #s3_key = aws_key_pattern.format(OID=oid)
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
                        logging.info(f"[EUID6] Uploaded OID '{oid}' for file: {file_path} to s3://{bucket_name}/{s3_key}")
                        uploaded_items.append({"oid": oid, "file_path": file_path})

                        try:
                            os.remove(file_path)
                            logging.info(f"Deleted file after processing: {file_path}")
                        except Exception as delete_error:
                            logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")

                    else:
                        logging.info(f"Key already exists: {s3_key}")
                        # Key is OID
                        # if the same OID is extracted again from a new file, the key will be identical, hence skipping to upload.
                else:
                    logging.warning(f"[EUID6] No OID extracted for: {file_path}")

    
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")

        logging.info(f"Batch uploaded items: {uploaded_items}")

    # DAG Chain

    #dated_folders = list_dated_folders_EUID6()
    #file_batches = chunk_files_in_folder.expand(folder_path=dated_folders)

    #extract_and_upload_EUID6.expand(file_batch=file_batches)
    #extract_and_upload_EUID6.expand(file_paths=chunk_files_in_folder.partial().expand(folder_path=dated_folders))

    #extract_and_upload_EUID6.expand(file_paths=chunk_files_in_folder.expand(folder_path=dated_folders))

    dated_folders = list_dated_folders_EUID6()
    all_batches = chunk_files_in_folder.expand(folder_path=dated_folders)
    extract_and_upload_EUID6.expand(file_batch=all_batches)
