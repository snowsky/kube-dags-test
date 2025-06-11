from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from functools import partial
import math
import logging
import os
from datetime import timedelta
from concurrent.futures import as_completed, ThreadPoolExecutor as PoolExecutor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
from typing import List


DEFAULT_DEST_FILES_DIRECTORY_ARCHIVE = '/source-biakonzasftp/C-127/archive'
DEFAULT_DEST_FILES_DIRECTORY = '/source-biakonzasftp/C-179/HL7v2In_to_Corepoint'

DEFAULT_AWS_FOLDER = 'HL7v2In/'

DEFAULT_MAX_POOL_WORKERS = 5
DEFAULT_MAX_TASKS = 200
DEFAULT_PAGE_SIZE = 1000
PARALLEL_TASK_LIMIT = 5
DEFAULT_AWS_TAG = ['CPProcessed', 'KONZAID']
DEFAULT_DB_CONN_ID = 'prd-az1-sqlw3-mysql-airflowconnection'
sql_hook = MySqlHook(mysql_conn_id=DEFAULT_DB_CONN_ID)

class BucketDetails:
    def __init__(self, aws_conn_id, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.s3_hook_kwargs = s3_hook_kwargs

AWS_BUCKETS = {
    'konzaandssigrouppipelines': BucketDetails('konzaandssigrouppipelines', {}),
}

default_args = {'owner': 'airflow'}

with DAG(
    dag_id='HL7v2_file_S3_move',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    start_date=datetime(2025, 1, 1),
    tags=['C-127', 'Canary','Staging_in_Prod'],
    concurrency=PARALLEL_TASK_LIMIT,
    params={
        "output_files_dir_path": Param(DEFAULT_DEST_FILES_DIRECTORY, type="string"),
        "aws_bucket": Param('konzaandssigrouppipelines', type="string"),
        "aws_folder": Param(DEFAULT_AWS_FOLDER, type="string"),
        "aws_tag": Param(DEFAULT_AWS_TAG, type="string"),
        "max_pool_workers": Param(DEFAULT_MAX_POOL_WORKERS, type="integer", minimum=0),
        "max_mapped_tasks": Param(DEFAULT_MAX_TASKS, type="integer", minimum=0),
        "page_size": Param(DEFAULT_PAGE_SIZE, type="integer", minimum=0)
    },
) as dag:
    dag_params = dag.params


@task
def process_s3_files(
    aws_bucket: str,
    aws_folder: str,
    output_files_dir_path: str,
    max_pool_workers: int
):
    s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
    files = s3_hook.list_keys(bucket_name=aws_bucket, prefix=aws_folder)

    if not files:
        logging.info("No files found in S3 folder.")
        return

    def process_file(file_key):
        try:
            # Download file
            file_name = os.path.basename(file_key)
            local_path = f"/tmp/{file_name}"
            s3_hook.download_file(
                key=file_key,
                bucket_name=aws_bucket,
                local_path=local_path
            )

            # Copy to destination directories
            dest1 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY, file_name)
            dest2 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY_ARCHIVE, file_name)
            os.makedirs(os.path.dirname(dest1), exist_ok=True)
            os.makedirs(os.path.dirname(dest2), exist_ok=True)
            os.system(f"cp {local_path} {dest1}")
            os.system(f"cp {local_path} {dest2}")

            # Delete from S3
            s3_hook.delete_objects(bucket=aws_bucket, keys=[file_key])
            logging.info(f"Processed and deleted {file_key}")
        except Exception as e:
            logging.error(f"Failed to process {file_key}: {e}")
            raise AirflowFailException(f"Error processing file {file_key}")

    with PoolExecutor(max_workers=max_pool_workers) as executor:
        futures = [executor.submit(process_file, file_key) for file_key in files]
        for future in as_completed(futures):
            future.result()  # Will raise exception if any task failed

process_s3_files(
    aws_bucket=dag_params["aws_bucket"],
    aws_folder=dag_params["aws_folder"],
    output_files_dir_path=dag_params["output_files_dir_path"],
    max_pool_workers=dag_params["max_pool_workers"]
)
