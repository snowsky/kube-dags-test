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
    dag_id='7_30_v1_hl7v2_msh4_oid',
    default_args=default_args,
    description='Parses HL7 files and resolves domain OID (MSH-4)',
    start_date=datetime(2025, 7, 10),
    catchup=False,
    max_active_runs=1,
    concurrency=100,
    tags=['C-179','hl7v2', 'Canary', 'Staging_in_Prod'],
    params={
        "max_workers": Param(50, type="integer", minimum=1),
        "batch_size": Param(500, type="integer", minimum=1)
    }
) as dag:

    @task(dag=dag)
    def list_and_chunk_files_EUID6(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        import re
    
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID6"
        batch_size = int(batch_size)
        max_mapped_tasks = 1024
    
        # Pattern to match dated folders like 20250729
        date_folder_pattern = re.compile(r"^\d{8}$")
    
        all_chunks = []
        folder_count = 0
    
        for entry in sorted(os.listdir(base_dir)):
            subfolder_path = os.path.join(base_dir, entry)
    
            if os.path.isdir(subfolder_path) and date_folder_pattern.match(entry):
                folder_count += 1
                current_folder_files = []
    
                logging.info(f"[EUID6] Scanning dated folder: {subfolder_path}")
                for f in os.listdir(subfolder_path):
                    if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f:
                        full_path = os.path.join(subfolder_path, f)
                        current_folder_files.append(full_path)
    
                if not current_folder_files:
                    logging.warning(f"[EUID6] No eligible files in: {subfolder_path}")
                    continue
    
                if len(current_folder_files) > max_mapped_tasks * batch_size:
                    folder_batch_size = (len(current_folder_files) // max_mapped_tasks) + 1
                else:
                    folder_batch_size = batch_size
    
                folder_chunks = [
                    current_folder_files[i:i + folder_batch_size]
                    for i in range(0, len(current_folder_files), folder_batch_size)
                ]
                all_chunks.extend(folder_chunks)
    
                logging.info(
                    f"[EUID6] Folder: {entry} | Files: {len(current_folder_files)} | "
                    f"Batch size: {folder_batch_size} | Batches: {len(folder_chunks)}"
                )
    
        if not all_chunks:
            logging.warning(f"[EUID6] No valid .hl7/.txt/no-extension files found in any dated folder under {base_dir}")
            return []
    
        logging.info(f"[EUID6] Finished. Dated folders scanned: {folder_count} | Total file batches: {len(all_chunks)}")
        return all_chunks
        
    @task(dag=dag)
    def extract_and_upload_EUID6(file_paths: List[str]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        for file_path in file_paths:
            try:
                logging.info(f"[EUID6] Processing file: {file_path}")
                oid = get_domain_oid_from_hl7v2_msh4_with_crosswalk_fallback(file_path)
                if oid:
                    s3_key = aws_key_pattern.format(OID=oid)
                    key_exists = s3_hook.check_for_key(s3_key, bucket_name=bucket_name)
    
                    if not key_exists:
                        s3_hook.load_string(
                            string_data=oid,
                            key=s3_key,
                            bucket_name=bucket_name,
                            **s3_hook_kwargs
                        )
                        logging.info(f"[EUID6] Uploaded OID '{oid}' for file: {file_path} to s3://{bucket_name}/{s3_key}")
                        #logging.info(f"Uploaded OID string to s3://{bucket_name}/{s3_key}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
                        #pass
    
                    uploaded_items.append({"oid": oid, "file_path": file_path})
    
                    # Delete regardless of upload or existing key
                    #try:
                        #os.remove(file_path)
                        #logging.info(f"Deleted file after processing: {file_path}")
                    #except Exception as delete_error:
                        #logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
                   
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")
        

    # DAG Chain

    file_batches_EUID6 = list_and_chunk_files_EUID6(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID6.expand(file_paths=file_batches_EUID6)
