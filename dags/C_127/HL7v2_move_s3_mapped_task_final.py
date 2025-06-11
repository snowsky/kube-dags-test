from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from io import BytesIO
import os
import logging

# Constants
DEFAULT_DEST_FILES_DIRECTORY_ARCHIVE = '/source-biakonzasftp/C-127/archive'
DEFAULT_DEST_FILES_DIRECTORY = '/source-biakonzasftp/C-179/HL7v2In_to_Corepoint'
DEFAULT_AWS_FOLDER = 'HL7v2In/'
DEFAULT_MAX_POOL_WORKERS = 5
DEFAULT_MAX_TASKS = 200
DEFAULT_PAGE_SIZE = 100
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
    dag_id='HL7v2_move_s3_mapped_task_final',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 1, 1),
    tags=['C-127', 'Canary', 'Staging_in_Prod'],
    concurrency=PARALLEL_TASK_LIMIT,
    max_active_runs=1,
    catchup=False,
    params={
        "output_files_dir_path": Param(DEFAULT_DEST_FILES_DIRECTORY, type="string"),
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

        logging.info(f"Runtime page_size: {page_size}")

        s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
        all_keys = s3_hook.list_keys(bucket_name=aws_bucket, prefix=aws_folder) or []
        files = [key for key in all_keys if not key.endswith('/')]
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
    
                dest1 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY, dated_subfolder)
                dest2 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY_ARCHIVE, dated_subfolder)
    
                os.makedirs(os.path.dirname(dest1), exist_ok=True)
                os.makedirs(os.path.dirname(dest2), exist_ok=True)
    
                # Download file into memory
                s3_key = s3_hook.get_key(key=file_key, bucket_name=aws_bucket)
                file_obj = BytesIO()
                s3_key.download_fileobj(file_obj)
                file_obj.seek(0)
    
                # Write to both destinations
                with open(dest1, 'wb') as f1, open(dest2, 'wb') as f2:
                    data = file_obj.read()
                    f1.write(data)
                    f2.write(data)
    
                # Log to database
                log_to_database(file_key)
    
                # Delete from S3
                s3_hook.delete_objects(bucket=aws_bucket, keys=[file_key])
                logging.info(f"Processed and deleted {file_key}")
    
            except Exception as e:
                logging.error(f"Failed to process {file_key}: {e}")
                raise AirflowFailException(f"Error processing file {file_key}")
    

    # DAG task wiring
    file_batches = list_s3_file_batches()

    process_file_batch.expand(file_keys=file_batches)
