from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from concurrent.futures import as_completed, ThreadPoolExecutor as PoolExecutor
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
    dag_id='HL7v2_file_S3_move',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    tags=['C-127', 'Canary', 'Staging_in_Prod'],
    concurrency=PARALLEL_TASK_LIMIT,
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
    dag_params = dag.params

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


    def chunk_list(lst, chunk_size):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    @task
    def process_s3_files(
        aws_bucket: str,
        aws_folder: str,
        output_files_dir_path: str,
        max_pool_workers: int,
        page_size: int
    ):
        s3_hook = S3Hook(aws_conn_id=AWS_BUCKETS[aws_bucket].aws_conn_id)
        files = s3_hook.list_keys(bucket_name=aws_bucket, prefix=aws_folder)
    
        if not files:
            logging.info("No files found in S3 folder.")
            return
    
        def log_to_database(file_key):
            # Replace with actual DB logging logic
            logging.info(f"Logging to DB: {file_key}")
            # Example: db_hook.run("INSERT INTO processed_files_log (file_key) VALUES (%s)", parameters=(file_key,))
    
        def process_file(file_key):
            try:
                s3_object = s3_hook.get_key(key=file_key, bucket_name=aws_bucket)
                tagging = s3_object.Object().Tagging().tag_set
                tag_dict = {tag['Key']: tag['Value'] for tag in tagging}
    
                if 'CPProcessed' in tag_dict:
                    logging.info(f"Skipping {file_key} — already tagged with CPProcessed.")
                    return
    
                file_name = os.path.basename(file_key)
    
                if 'KONZAID' in tag_dict and tag_dict['KONZAID']:
                    konza_id = tag_dict['KONZAID']
                    file_name = f"KONZA__{konza_id}__{file_name}"
    
                today_str = datetime.today().strftime('%Y%m%d')
                dated_subfolder = os.path.join(today_str, file_name)
    
                dest1 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY, dated_subfolder)
                dest2 = os.path.join(DEFAULT_DEST_FILES_DIRECTORY_ARCHIVE, dated_subfolder)
    
                os.makedirs(os.path.dirname(dest1), exist_ok=True)
                os.makedirs(os.path.dirname(dest2), exist_ok=True)
    
                s3_hook.download_file(key=file_key, bucket_name=aws_bucket, local_path=dest1)
                s3_hook.download_file(key=file_key, bucket_name=aws_bucket, local_path=dest2)
    
                log_to_database(file_key)
    
                s3_hook.delete_objects(bucket=aws_bucket, keys=[file_key])
                logging.info(f"Processed and deleted {file_key}")
            except Exception as e:
                logging.error(f"Failed to process {file_key}: {e}")
                raise AirflowFailException(f"Error processing file {file_key}")
    
        for batch in chunk_list(files, page_size):
            with PoolExecutor(max_workers=max_pool_workers) as executor:
                futures = [executor.submit(process_file, file_key) for file_key in batch]
                for future in as_completed(futures):
                    future.result()


    process_s3_files(
        aws_bucket=dag_params["aws_bucket"],
        aws_folder=dag_params["aws_folder"],
        output_files_dir_path=dag_params["output_files_dir_path"],
        max_pool_workers=dag_params["max_pool_workers"],
        page_size=dag_params["page_size"]
    )
