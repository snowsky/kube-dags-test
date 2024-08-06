import os
import logging
from airflow.hooks.base_hook import BaseHook
import boto3
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import tempfile

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'L_215_s3bucket_targeted_retrieval',
    default_args=default_args,
    description='A simple DAG to move files from an S3 subfolder to a mounted drive location',
    schedule_interval='@hourly',  # Set to run hourly
    start_date=datetime(2024,8,5),
    tags=['example'],
)
# Define the S3 bucket name and subfolder
BUCKET_NAME = 'konzaandssigroupncqa'
S3_SUBFOLDER = ''
LOCAL_DESTINATION = '/source-biakonzasftp/L-215/' #PRD is '/source-biakonzasftp/C-9/optout_load/' #DEV is '/data/biakonzasftp/L-215/'
TEMP_DIRECTORY = '/source-biakonzasftp/airflow_temp/'

# Task to list files in the S3 subfolder
def list_files_in_s3(**kwargs):
    hook = S3Hook(aws_conn_id='konzaandssigroupncqa')
    files = hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_SUBFOLDER)
    return files

list_files_task = PythonOperator(
    task_id='list_files',
    python_callable=list_files_in_s3,
    provide_context=True,
    dag=dag,
)

def move_files_to_local(**kwargs):
    # Retrieve connection details from Airflow
    connection = BaseHook.get_connection('konzaandssigroupncqa')
    
    # Use the connection details to configure Boto3
    s3 = boto3.client(
        's3',
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=connection.extra_dejson.get('region_name')
    )
    
    files = kwargs['ti'].xcom_pull(task_ids='list_files')
    
    for file_key in files:
        # Skip files that start with "airflow"
        if file_key.startswith('airflow_tmp_'):
            logging.info(f'Skipping {file_key} as it starts with "airflow"')
            continue
        
        # Replicate the S3 directory structure in the local destination
        local_file_path = os.path.join(LOCAL_DESTINATION, file_key)
        local_dir = os.path.dirname(local_file_path)
        
        # Ensure the local directory structure exists
        os.makedirs(local_dir, exist_ok=True)
        
        # Generate a unique temporary file path
        temp_file_name = next(tempfile._get_candidate_names())
        temp_file_path = os.path.join(TEMP_DIRECTORY, temp_file_name)
        
        try:
            # Ensure the TEMP_DIRECTORY exists
            os.makedirs(TEMP_DIRECTORY, exist_ok=True)
            
            logging.info(f'Attempting to download {file_key} to {temp_file_path}')
            # Use Boto3 to download the file
            s3.download_file(BUCKET_NAME, file_key, temp_file_path)
            logging.info(f'Successfully downloaded {file_key} to {temp_file_path}')
            
            # Check if the temporary file exists before moving
            if os.path.exists(temp_file_path):
                # Move the file from the temp directory to the local destination
                os.rename(temp_file_path, local_file_path)
                logging.info(f'Successfully moved {file_key} to {local_file_path}')
            else:
                logging.error(f'Failed to download {file_key} to {temp_file_path}')
        except Exception as e:
            logging.error(f'Error while attempting to download {file_key}: {e}')
            # Ensure that temporary file is removed in case of failure
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

move_files_task = PythonOperator(
    task_id='move_files',
    python_callable=move_files_to_local,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
list_files_task >> move_files_task

if __name__ == "__main__":
    dag.cli()
