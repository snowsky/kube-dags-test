from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the paths
pathIaaSLogs = '/source-biakonzasftp/S-6/IaaS_Logs/'
parquet_logs_master = os.path.join(pathIaaSLogs, 'parquet-logs-master')

# Initialize counters
file_count = 0
parquet_count = 0

# Initialize a list to store file paths and contents
file_data = []

# Define the failure callback function
def failure_callback(context):
    dag_name = context['dag'].dag_id
    dag_file_path = context['dag'].fileloc
    send_email(
        to='networksecurity@konza.org',
        subject=f'Task Failed in DAG: {dag_name}',
        html_content=f"Task {context['task_instance_key_str']} failed in DAG: {dag_name}. DAG source file: {dag_file_path}. Check the logs for more details."
    )

# Function to save data to a Parquet file
def save_to_parquet(data, partition_name, parquet_count):
    df = pd.DataFrame(data)
    df['index_update'] = partition_name  # Add the index_update column
    directory_path = os.path.join(parquet_logs_master, partition_name)
    os.makedirs(directory_path, exist_ok=True)
    #parquet_path = os.path.join(directory_path, f'file_{parquet_count}.parquet')
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    parquet_path = os.path.join(directory_path, f'file_{parquet_count}_{timestamp}.parquet')
    
    df.to_parquet(parquet_path)
    logger.info(f'Saved {len(data)} rows to {parquet_path}')
    
    ## Delete the original files
    #for item in data:
    #    os.remove(item['file_path'])
    #    logger.info(f'Deleted {item["file_path"]}')

# Function to process files
logger.info(f'File Count at outside of def: {file_count}')
logger.info(f'File Count at outside of def: {file_count}')
def process_files():
    global file_count, parquet_count, file_data, partition_name
    logger.info(f'File Count at global variable in processdef: {file_count}')
    print('starting processing')
    
    def scan_directory(directory):
        global file_count, parquet_count, file_data, partition_name
        logger.info(f'File Count at scan_directorydef: {file_count}')
        print(f'starting directory {directory}')
        for entry in os.scandir(directory):
            print(f'starting entry {entry} with entry path {entry.path}')
            if entry.is_dir() and 'parquet-logs-master' not in entry.path:
                print(f'starting entry {entry} after check')
                scan_directory(entry.path)
                # Check if the directory is empty after processing
                if not os.listdir(entry.path):
                    print(f'Deleting empty directory: {entry.path}')
                    os.rmdir(entry.path)
            elif entry.is_file() and entry.name.endswith('.json'):
                print(f'starting file {entry.path}')
                file_path = entry.path
                modified_time = os.path.getmtime(file_path)
                partition_name = datetime.fromtimestamp(modified_time).strftime('%Y-%m')
                print(f'Partition Name: {partition_name}')
                
                with open(file_path, 'r') as f:
                    file_content = f.read()
                file_data.append({'file_path': file_path, 'file_content': file_content})
                logger.info(f'Adding {file_path} to dataframe')
                print(file_path)
                file_count += 1

                if file_count >= 1000:  # Normally 1M for 1M rows per parquet file
                    logger.info(f'File Count max Reached for reset: {file_count}')
                    parquet_count += 1
                    save_to_parquet(file_data, partition_name, parquet_count)
                    file_data = []
                    file_count = 0
                    return  # Stop processing after the first 1,000 files

    scan_directory(pathIaaSLogs)

    # Save any remaining data
    if file_data:
        parquet_count += 1
        save_to_parquet(file_data, partition_name, parquet_count)
        
# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'on_failure_callback': failure_callback,
}

dag = DAG(
    'Log_Translation_to_Parquet',
    default_args=default_args,
    description='This DAG collects logs to compress for trino table queries',
    schedule_interval='None',#'@daily'
    catchup=False,
    tags=['S-6'],
)
# Task 1: Copy the syslog file from SFTP to network file path
process_files_to_parquet_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    dag=dag,
    on_failure_callback=failure_callback,
)
