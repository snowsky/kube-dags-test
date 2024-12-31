import logging
import re
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
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
    'azcopy_containers',
    default_args=default_args,
    description='Copy containers from source to destination storage account',
    schedule_interval=None,
    tags=['S-6'],
    start_date=days_ago(1),
)

# Variables
source_account_url = "https://logauditdata.blob.core.windows.net"
source_account_name = "logauditdata"
destination_base_url = "https://biakonzasftp.blob.core.windows.net/airflow"

# Retrieve the source storage account key from Airflow connection
source_connection = BaseHook.get_connection('logauditdata-blob-core-windows-net')
source_account_key = source_connection.password

# Retrieve the destination storage account key from Airflow connection
destination_connection = BaseHook.get_connection('biakonzasftp-blob-core-windows-net')
destination_account_key = destination_connection.password

# Get the list of containers in the source storage account
list_containers_cmd = f"az storage container list --account-name {source_account_name} --account-key {source_account_key} --query '[].name' -o tsv"

# Define the task to list containers
list_containers = BashOperator(
    task_id='list_containers',
    bash_command=list_containers_cmd,
    dag=dag,
)

# Define the task to copy each container
def create_copy_task(container_name):
    # Log the original container_name
    logger.info(f"Original container_name: {container_name}")
    
    # Replace any characters that are not alphanumeric, dashes, dots, or underscores with underscores
    sanitized_container_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', container_name)
    
    # Log the sanitized container_name
    logger.info(f"Sanitized container_name: {sanitized_container_name}")
    
    copy_command = f"azcopy copy '{source_account_url}/{container_name}' '{destination_base_url}/{container_name}' --recursive"
    logger.info(f"Executing command: {copy_command}")
    
    return BashOperator(
        task_id=f'copy_container_{sanitized_container_name}',
        bash_command=copy_command,
        dag=dag,
    )

# Create a dynamic task for each container
containers = list_containers_cmd.split()
for container in containers:
    copy_task = create_copy_task(container)
    list_containers >> copy_task
