from airflow import DAG  
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#from airflow.providers.sftp.operators.sftp import SFTPOperation
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowFailException
from functools import partial
import boto3
import math
import logging
import re
from datetime import datetime

# If we want to utilise ProcessPoolExecutor we need to set
# AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER = true
from concurrent.futures import as_completed, ThreadPoolExecutor as PoolExecutor
 
class DebugS3ToSFTPOperator(S3ToSFTPOperator):
    def execute(self, context):
        # Log the S3 key and SFTP path
        logging.info(f"S3 Key: {self.s3_key}")
        logging.info(f"SFTP Path: {self.sftp_path}")
        
        # Call the parent class's execute method
        super().execute(context)


# Define the DAG
default_args = {
    'owner': 'airflow',
    #'depends_on_past': False,
    #'start_date': datetime(2024, 10, 15),
    #'retries': 3,
}
dag = DAG(
    'HL7v3In_s3_to_sftp_delete_from_s3',
    default_args=default_args,
    description='Retrieve files from SFTP, deliver to network path and S3 and deletes the files from S3',
    schedule_interval=None,
    catchup=False,
    tags=['C-128']

)
BUCKET_NAME = 'konzaandssigrouppipelines'
S3_SUBFOLDER = 'HL7v3In/'

@task(dag=dag)
def list_files_in_s3():
    hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    files = hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_SUBFOLDER)
    logging.info(f'Files in S3: {files}')
    return files

@task(dag=dag)
def transfer_file_to_sftp(file_key):
    sanitized_task_id = re.sub(r'[^a-zA-Z0-9_.-]', '_', f'move_file_{file_key}')
    logging.info(f'Transferring file with ID: {sanitized_task_id}')
    
    sftp_path = f'C-128/C_128_test_delivery/HL7v3In/{file_key.split("/")[-1]}'
    logging.info(f'SFTP Path: {sftp_path}')

    try:
        copy_task = DebugS3ToSFTPOperator(
            task_id=sanitized_task_id,
            sftp_conn_id='sftp_airflow',
            sftp_path=sftp_path,  
            s3_bucket=BUCKET_NAME,
            s3_key=file_key,
            aws_conn_id='konzaandssigrouppipelines',
            dag=dag,
        )
        copy_task.execute({})  # Execute the task immediately
        return file_key  # Return the file key if successful
    except Exception as e:
        logging.error(f'Failed to transfer {file_key}: {e}')
        return None  # Return None if failed


def delete_files_from_s3(**kwargs):
    connection = BaseHook.get_connection('konzaandssigrouppipelines')
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=connection.extra_dejson.get('region_name')
    )
    # Pull successful file keys from XCom
    successful_files = kwargs['ti'].xcom_pull(task_ids='transfer_file_to_sftp')
    for file_key in successful_files:
        if file_key:  # Only delete if the file_key is not None
            try:
                logging.info(f'Attempting to delete {file_key} from S3')
                s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
                logging.info(f'Successfully deleted {file_key} from S3')
            except Exception as e:
                logging.error(f'Error while attempting to delete {file_key}: {e}')

delete_files_task = PythonOperator(
    task_id='delete_files',
    python_callable=delete_files_from_s3,
    provide_context=True,
    dag=dag,
)

# Define the workflow
files = list_files_in_s3()
transfer_tasks = transfer_file_to_sftp.expand(file_key=files)
transfer_tasks >> delete_files_task  # Set dependency

if __name__ == "__main__":
    dag.cli()
