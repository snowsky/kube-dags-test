import logging
import re
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
from datetime import timedelta

# Set up logging
logger = logging.getLogger('azcopy_dag')
logger.setLevel(logging.INFO)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'azcopy_containers_airflow_native',
    default_args=default_args,
    description='Copy containers from source to destination storage account',
    schedule_interval=None,
    tags=['S-6'],
    start_date=days_ago(1),
)

# Variables
source_account_name = "logauditdata"
destination_account_name = "biakonzasftp"

# Retrieve the source storage account key from Airflow connection
source_connection = BaseHook.get_connection('logauditdata-blob-core-windows-net')
source_account_key = source_connection.password

# Retrieve the destination storage account key from Airflow connection
destination_connection = BaseHook.get_connection('biakonzasftp-blob-core-windows-net')
destination_account_key = destination_connection.password

# Define the task to list containers
def list_containers():
    hook = WasbHook(wasb_conn_id='logauditdata-blob-core-windows-net')
    containers = hook.get_conn().list_containers()
    container_names = [container.name for container in containers]
    return container_names

list_containers_task = PythonOperator(
    task_id='list_containers',
    python_callable=list_containers,
    dag=dag,
)

# Define the task to copy each container
def copy_container(container_name):
    source_hook = WasbHook(wasb_conn_id='logauditdata-blob-core-windows-net')
    destination_hook = WasbHook(wasb_conn_id='biakonzasftp-blob-core-windows-net')
    
    # Log the original container_name
    logger.info(f"Original container_name: {container_name}")
    
    # Replace any characters that are not alphanumeric, dashes, dots, or underscores with underscores
    sanitized_container_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', container_name)
    
    # Log the sanitized container_name
    logger.info(f"Sanitized container_name: {sanitized_container_name}")
    
    blobs = source_hook.get_blobs_list(container_name)
    for blob in blobs:
        source_blob = f"{container_name}/{blob.name}"
        destination_blob = f"{sanitized_container_name}/{blob.name}"
        try:
            source_hook.copy_blob(source_blob, destination_account_name, destination_blob)
            logger.info(f"Copied {source_blob} to {destination_blob}")
        except Exception as e:
            logger.error(f"Failed to copy {source_blob} to {destination_blob}: {e}")

# Create a dynamic task for each container
def create_copy_tasks():
    containers = list_containers()
    for container in containers:
        copy_task = PythonOperator(
            task_id=f'copy_container_{container}',
            python_callable=copy_container,
            op_args=[container],
            dag=dag,
        )
        list_containers_task >> copy_task

create_copy_tasks()
