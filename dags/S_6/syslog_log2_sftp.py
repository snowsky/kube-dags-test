from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime
import os
import shutil
import pwd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the failure callback function
def failure_callback(context):
    send_email(
        to='networksecurity@konza.org',
        subject='Task Failed',
        html_content=f"Task {context['task_instance_key_str']} failed. Check the logs for more details."
    )

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
    'on_failure_callback': failure_callback,
}

dag = DAG(
    'prd-az1-log2_syslog_to_local_sftp',
    default_args=default_args,
    description='This DAG retrieves some firewall logs from a VM where they are collected and stores it for future audits with HITRUST implications',
    schedule_interval='@daily',
    catchup=False,
    tags=['S-6'],
)

# Define network file path
network_file_path = '/source-biakonzasftp/S-6/firewall_logs/'
file_name = 'syslog'

# Python function to copy files from SFTP to network file path
def copy_to_network_path(sftp_conn_id, sftp_path, network_path):
    sftp_hook = SFTPHook(sftp_conn_id)
    file_names = sftp_hook.list_directory(sftp_path)

    current_date = datetime.now()
    subfolder = current_date.strftime('%Y%m')
    day_stamp = current_date.strftime('%d')
    network_path_with_date = os.path.join(network_path, subfolder)

    if not os.path.exists(network_path_with_date):
        os.makedirs(network_path_with_date)

    if sftp_hook.isfile(os.path.join(sftp_path, file_name)):
        local_file_path = os.path.join(network_path_with_date, f'{file_name}_{day_stamp}')
        
        # Download the file first to a temporary location in the user's home directory
        home_directory = os.path.expanduser("~")
        temp_local_file_path = os.path.join(home_directory, f'{file_name}_{day_stamp}')
        sftp_hook.retrieve_file(os.path.join(sftp_path, file_name), temp_local_file_path)
        
        # Move the file to the network path
        shutil.move(temp_local_file_path, local_file_path)
        
        logger.info(f'Copied {file_name} to {network_path_with_date} with day stamp {day_stamp} via home directory')

        # Command to copy the file using sudo cp
        cp_command = f"sudo cp {temp_local_file_path} {home_directory}"
        logger.info(f'Executing command: {cp_command}')
        os.system(cp_command)
        logger.info(f'Copied {temp_local_file_path} to {home_directory} using sudo cp command')

        # Change the owner of the file to the current user
        current_user = pwd.getpwuid(os.getuid()).pw_name
        chown_command = f"sudo chown {current_user}:{current_user} {os.path.join(home_directory, file_name)}"
        logger.info(f'Executing command: {chown_command}')
        os.system(chown_command)
        logger.info(f'Changed owner of {os.path.join(home_directory, file_name)} to {current_user}')

# Task 1: Copy files from SFTP to network file path
copy_to_network_task = PythonOperator(
    task_id='copy_files_to_network',
    python_callable=copy_to_network_path,
    op_kwargs={
        'sftp_conn_id': 'prd-az1-logs2',
        'sftp_path': '/var/log/',
        'network_path': network_file_path,
    },
    dag=dag,
    on_failure_callback=failure_callback,
)
