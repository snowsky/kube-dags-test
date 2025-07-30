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
    dag_id='hl7v2_msh4_oid',
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
    def list_and_chunk_files_EUID1(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        #import os

        #base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID2"
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID1"

        batch_size = int(batch_size)
        max_mapped_tasks = 1024

        all_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            #if f.endswith(".hl7") or f.endswith(".txt")
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f

        ]

        if len(all_files) > max_mapped_tasks:
            batch_size = max(batch_size, (len(all_files) // max_mapped_tasks) + 1)

        chunks = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
        logging.info(f"Total files: {len(all_files)} | Batch size: {batch_size} | Total batches: {len(chunks)}")
        return chunks
        
    @task(dag=dag)
    def extract_and_upload_EUID1(file_paths: List[str]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        for file_path in file_paths:
            try:
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
                        logging.info(f"Uploaded OID string to s3://{bucket_name}/{s3_key}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
    
                    uploaded_items.append({"oid": oid, "file_path": file_path})
    
                    # Delete regardless of upload or existing key
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted file after processing: {file_path}")
                    except Exception as delete_error:
                        logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")
    
    @task(dag=dag)
    def list_and_chunk_files_EUID2(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        #import os

        #base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID2"
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID2"

        batch_size = int(batch_size)
        max_mapped_tasks = 1024

        all_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            #if f.endswith(".hl7") or f.endswith(".txt")
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f

        ]

        if len(all_files) > max_mapped_tasks:
            batch_size = max(batch_size, (len(all_files) // max_mapped_tasks) + 1)

        chunks = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
        logging.info(f"Total files: {len(all_files)} | Batch size: {batch_size} | Total batches: {len(chunks)}")
        return chunks
        
    @task(dag=dag)
    def extract_and_upload_EUID2(file_paths: List[str]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        for file_path in file_paths:
            try:
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
                        logging.info(f"Uploaded OID string to s3://{bucket_name}/{s3_key}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
    
                    uploaded_items.append({"oid": oid, "file_path": file_path})
    
                    # Delete regardless of upload or existing key
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted file after processing: {file_path}")
                    except Exception as delete_error:
                        logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")

    @task(dag=dag)
    def list_and_chunk_files_EUID3(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        #import os

        #base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID2"
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID3"

        batch_size = int(batch_size)
        max_mapped_tasks = 1024

        all_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            #if f.endswith(".hl7") or f.endswith(".txt")
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f

        ]

        if len(all_files) > max_mapped_tasks:
            batch_size = max(batch_size, (len(all_files) // max_mapped_tasks) + 1)

        chunks = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
        logging.info(f"Total files: {len(all_files)} | Batch size: {batch_size} | Total batches: {len(chunks)}")
        return chunks
        
    @task(dag=dag)
    def extract_and_upload_EUID3(file_paths: List[str]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        for file_path in file_paths:
            try:
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
                        logging.info(f"Uploaded OID string to s3://{bucket_name}/{s3_key}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
    
                    uploaded_items.append({"oid": oid, "file_path": file_path})
    
                    # Delete regardless of upload or existing key
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted file after processing: {file_path}")
                    except Exception as delete_error:
                        logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")

    @task(dag=dag)
    def list_and_chunk_files_EUID4(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        #import os

        #base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID2"
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID4"

        batch_size = int(batch_size)
        max_mapped_tasks = 1024

        all_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            #if f.endswith(".hl7") or f.endswith(".txt")
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f

        ]

        if len(all_files) > max_mapped_tasks:
            batch_size = max(batch_size, (len(all_files) // max_mapped_tasks) + 1)

        chunks = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
        logging.info(f"Total files: {len(all_files)} | Batch size: {batch_size} | Total batches: {len(chunks)}")
        return chunks
        
    @task(dag=dag)
    def extract_and_upload_EUID4(file_paths: List[str]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        for file_path in file_paths:
            try:
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
                        logging.info(f"Uploaded OID string to s3://{bucket_name}/{s3_key}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
    
                    uploaded_items.append({"oid": oid, "file_path": file_path})
    
                    # Delete regardless of upload or existing key
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted file after processing: {file_path}")
                    except Exception as delete_error:
                        logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")

    @task(dag=dag)
    def list_and_chunk_files_EUID5(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        #import os

        #base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID2"
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID5"

        batch_size = int(batch_size)
        max_mapped_tasks = 1024

        all_files = [
            os.path.join(base_dir, f)
            for f in os.listdir(base_dir)
            #if f.endswith(".hl7") or f.endswith(".txt")
            if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f

        ]

        if len(all_files) > max_mapped_tasks:
            batch_size = max(batch_size, (len(all_files) // max_mapped_tasks) + 1)

        chunks = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
        logging.info(f"Total files: {len(all_files)} | Batch size: {batch_size} | Total batches: {len(chunks)}")
        return chunks
        
    @task(dag=dag)
    def extract_and_upload_EUID5(file_paths: List[str]):
        bucket_name = 'com-ssigroup-insight-attribution-data'
        bucket_config = AWS_BUCKETS[bucket_name]
        aws_conn_id = bucket_config.aws_conn_id
        aws_key_pattern = bucket_config.aws_key_pattern
        s3_hook_kwargs = bucket_config.s3_hook_kwargs
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    
        uploaded_items = []
    
        for file_path in file_paths:
            try:
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
                        logging.info(f"Uploaded OID string to s3://{bucket_name}/{s3_key}")
                    else:
                        logging.info(f"Key already exists: {s3_key}")
    
                    uploaded_items.append({"oid": oid, "file_path": file_path})
    
                    # Delete regardless of upload or existing key
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted file after processing: {file_path}")
                    except Exception as delete_error:
                        logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")

        @task(dag=dag)
    def list_and_chunk_files_EUID6(batch_size: Union[str, int] = 1000) -> List[List[str]]:
        #base_dir = "/data/biakonzasftp/C-179/OB To SSI EUID6_test"
        base_dir = "/source-biakonzasftp/C-179/OB To SSI EUID6"

        batch_size = int(batch_size)
        max_mapped_tasks = 1024
    
        all_files = []
        checked_dirs = []
    
        # Recursively walk through all dated subfolders
        for root, dirs, files in os.walk(base_dir):
            checked_dirs.append(root)
            for f in files:
                if f.endswith(".hl7") or f.endswith(".txt") or '.' not in f:
                    all_files.append(os.path.join(root, f))
    
        if not all_files:
            logging.warning(f"[EUID6] No .hl7 or .txt files found in {base_dir}. Checked subfolders: {checked_dirs}")
            return []
    
        if len(all_files) > max_mapped_tasks:
            batch_size = max(batch_size, (len(all_files) // max_mapped_tasks) + 1)
    
        chunks = [all_files[i:i + batch_size] for i in range(0, len(all_files), batch_size)]
        logging.info(f"[EUID6] Total files: {len(all_files)} | Batch size: {batch_size} | Total batches: {len(chunks)}")
        return chunks
        
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
                    try:
                        os.remove(file_path)
                        logging.info(f"Deleted file after processing: {file_path}")
                    except Exception as delete_error:
                        logging.warning(f"Processed but failed to delete: {file_path} | {delete_error}")
                   
            except Exception as e:
                logging.warning(f"Failed: {file_path} | {e}")
    
        logging.info(f"Batch uploaded items: {uploaded_items}")
        

    # DAG Chain
    file_batches_EUID1 = list_and_chunk_files_EUID1(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID1.expand(file_paths=file_batches_EUID1)
    
    file_batches_EUID2 = list_and_chunk_files_EUID2(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID2.expand(file_paths=file_batches_EUID2)

    file_batches_EUID3 = list_and_chunk_files_EUID3(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID3.expand(file_paths=file_batches_EUID3)

    file_batches_EUID4 = list_and_chunk_files_EUID4(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID4.expand(file_paths=file_batches_EUID4)

    file_batches_EUID5 = list_and_chunk_files_EUID5(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID5.expand(file_paths=file_batches_EUID5) 

    file_batches_EUID6 = list_and_chunk_files_EUID6(batch_size="{{ params.batch_size }}")
    extract_and_upload_EUID6.expand(file_paths=file_batches_EUID6)
