from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.hooks.base_hook import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from io import BytesIO
import os
import logging

# Constants
DEFAULT_AZURE_CONTAINER = 'content'
DEFAULT_DEST_PATH_ARCHIVE = 'C-127/archive'
DEFAULT_DEST_PATH = 'C-179/HL7v2In_to_Corepoint'
DEFAULT_AWS_FOLDER = 'HL7v2In/'
DEFAULT_MAX_POOL_WORKERS = 5
DEFAULT_MAX_TASKS = 200
DEFAULT_PAGE_SIZE = 10000
PARALLEL_TASK_LIMIT = 30
DEFAULT_AWS_TAG = ['CPProcessed', 'KONZAID']
DEFAULT_DB_CONN_ID = 'prd-az1-sqlw3-mysql-airflowconnection'
AZURE_CONNECTION_NAME = 'biakonzasftp-blob-core-windows-net'

sql_hook = MySqlHook(mysql_conn_id=DEFAULT_DB_CONN_ID)

# Get Azure connection string once at DAG definition time

def get_azure_connection_string(conn_id: str) -> str:
    """
    Retrieve the Azure Blob Storage connection string from Airflow connections.
    Args:
        conn_id: The Airflow connection ID
    Returns:
        The Azure connection string
    """
    connection = BaseHook.get_connection(conn_id)
    conn_string = connection.extra_dejson.get("connection_string")
    if not conn_string:
        raise ValueError(f"No connection string found in connection {conn_id}")
    return conn_string

AZURE_CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)

class BucketDetails:
    def __init__(self, aws_conn_id, s3_hook_kwargs):
        self.aws_conn_id = aws_conn_id
        self.s3_hook_kwargs = s3_hook_kwargs

AWS_BUCKETS = {
    'konzaandssigrouppipelines': BucketDetails('konzaandssigrouppipelines', {}),
}

default_args = {'owner': 'airflow'}

with DAG(
    dag_id='Test_HL7v2_move_s3_mapped_task_final',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 1, 1),
    tags=['C-127', 'Canary', 'Staging_in_Prod'],
    concurrency=PARALLEL_TASK_LIMIT,
    max_active_runs=1,
    catchup=False,
    params={
        "output_files_dir_path": Param(DEFAULT_DEST_PATH, type="string"),
        "aws_bucket": Param('konzaandssigrouppipelines', type="string"),
        "aws_folder": Param(DEFAULT_AWS_FOLDER, type="string"),
        "aws_tag": Param(DEFAULT_AWS_TAG, type="array"),
        "max_pool_workers": Param(DEFAULT_MAX_POOL_WORKERS, type="integer", minimum=0),
        "max_mapped_tasks": Param(DEFAULT_MAX_TASKS, type="integer", minimum=0),
        "page_size": Param(DEFAULT_PAGE_SIZE, type="integer", minimum=0)
    },
) as dag:

    @task
    def list_s3_file_batches(**kwargs) -> list[list[str]]:
        params = kwargs["params"]
        aws_bucket = params["aws_bucket"]
        aws_folder = params["aws_folder"]
        page_size = params["page_size"]
        FILE_LIMIT = 2000000
        MAX_KEYS_FOR_DISCOVERY = 1000000
    
        logging.info(f"Runtime page_size: {page_size}")
    
        s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
        s3_client = s3_hook.get_conn()
    
        # Step 1: List a limited number of keys using list_keys
        all_keys = s3_hook.list_keys(bucket_name=aws_bucket, prefix=aws_folder) or []
        keys = [key for key in all_keys if not key.endswith('/')] [:MAX_KEYS_FOR_DISCOVERY]
    
        logging.info(f"Sample keys retrieved: {keys[:10]}")
    
        # Step 2: Extract unique three-level prefixes
        subfolders = set()
        for key in keys:
            parts = key.split('/')
            logging.info(f"Key: {key} | Parts: {parts}")
            if len(parts) >= 4:
                subfolders.add(f"{parts[0]}/{parts[1]}/{parts[2]}/")
    
        logging.info(f"Discovered subfolders: {subfolders}")
    
        files = []
        paginator = s3_client.get_paginator('list_objects_v2')
    
        # Step 3: Iterate through subfolders and collect tagged files
        for sub_prefix in subfolders:
            for page in paginator.paginate(Bucket=aws_bucket, Prefix=sub_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('/'):
                        continue
                    tags = s3_client.get_object_tagging(Bucket=aws_bucket, Key=key).get('TagSet', [])
                    if any(tag['Key'] == 'CPProcessed' and tag['Value'].lower() == 'true' for tag in tags):
                        files.append(key)
                        if len(files) >= FILE_LIMIT:
                            break
                if len(files) >= FILE_LIMIT:
                    break
            if len(files) >= FILE_LIMIT:
                break
        logging.info(f'Total Files To Process: {len(files)}')
        def chunk_list(lst, size):
            return [lst[i:i + size] for i in range(0, len(lst), size)]
    
        return chunk_list(files, page_size)

    def log_to_database(file_key):
        try:
            sql_hook = MySqlHook(mysql_conn_id=DEFAULT_DB_CONN_ID)
    
            # Create table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS processed_files_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_key VARCHAR(1024) NOT NULL,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            sql_hook.run(create_table_sql)
    
            # Insert log entry
            insert_sql = """
                INSERT INTO processed_files_log (file_key)
                VALUES (%s)
            """
            sql_hook.run(insert_sql, parameters=(file_key,))
            logging.info(f"Logged {file_key} to database.")
        except Exception as e:
            logging.error(f"Failed to log {file_key} to database: {e}")
    
    @task
    def process_file_batch(file_keys: list[str], **kwargs):
        params = kwargs["params"]
        aws_bucket = params["aws_bucket"]
    
        s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        logging.info(f'Total Files To Process in batch: {len(file_keys)}')
        for file_key in file_keys:
            try:
                s3_client = s3_hook.get_conn()
                tagging = s3_client.get_object_tagging(Bucket=aws_bucket, Key=file_key)
                tag_dict = {tag['Key']: tag['Value'] for tag in tagging.get('TagSet', [])}
                logging.info(f"Tags for {file_key}: {tag_dict}")
    
                if 'CPProcessed' not in tag_dict:
                    logging.info(f"Skipping {file_key} — not tagged with CPProcessed.")
                    continue
    
                if file_key.endswith('/'):
                    logging.warning(f"Skipping directory-like key: {file_key}")
                    continue
    
                file_name = file_key.split('/')[-1] or f"unnamed_{hash(file_key)}"
    
                if 'KONZAID' in tag_dict and tag_dict['KONZAID']:
                    file_name = f"KONZA__{tag_dict['KONZAID']}__{file_name}"
    
                today_str = datetime.today().strftime('%Y%m%d')
                dated_subfolder = os.path.join(today_str, file_key)
    
                dest1_path = os.path.join(DEFAULT_DEST_PATH, dated_subfolder)
                dest2_path = os.path.join(DEFAULT_DEST_PATH_ARCHIVE, dated_subfolder)
    
                # Download file into memory
                s3_key = s3_hook.get_key(key=file_key, bucket_name=aws_bucket)
                file_obj = BytesIO()
                s3_key.download_fileobj(file_obj)
                file_obj.seek(0)
    
                # Upload to both destinations in Azure Blob Storage
                for dest_path in [dest1_path, dest2_path]:
                    blob_client = blob_service_client.get_blob_client(
                        container=DEFAULT_AZURE_CONTAINER,
                        blob=dest_path
                    )
                    blob_client.upload_blob(file_obj, overwrite=True)
                    logging.info(f"Uploaded file to Azure Blob Storage at {dest_path}")
                    file_obj.seek(0)  # Reset buffer position for next upload
    
                # Log to database
                log_to_database(file_key)
    
                # Delete from S3
                # s3_hook.delete_objects(bucket=aws_bucket, keys=[file_key])
                # logging.info(f"Processed and deleted {file_key}")
    
            except Exception as e:
                logging.error(f"Failed to process {file_key}: {e}")
                raise AirflowFailException(f"Error processing file {file_key}")
    

    # DAG task wiring
    file_batches = list_s3_file_batches()

    process_file_batch.expand(file_keys=file_batches[10])
