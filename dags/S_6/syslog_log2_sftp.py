from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Connection
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import os
import paramiko
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the failure callback function
def failure_callback(context):
    dag_name = context['dag'].dag_id
    dag_file_path = context['dag'].fileloc
    send_email(
        to='networksecurity@konza.org',
        subject=f'Task Failed in DAG: {dag_name}',
        html_content=f"Task {context['task_instance_key_str']} failed in DAG: {dag_name}. DAG source file: {dag_file_path}. Check the logs for more details."
    )

# Function to execute SSH command
def execute_ssh_command(hostname, username, key_file, command):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username=username, key_filename=key_file)
    stdin, stdout, stderr = ssh.exec_command(command)
    output = stdout.read().decode().strip()
    error = stderr.read().decode().strip()
    ssh.close()
    return output, error

# Python function to copy files from SFTP to network file path
def copy_to_network_path(sftp_conn_id, sftp_path, network_path):
    sftp_hook = SFTPHook(sftp_conn_id)
    connection = BaseHook.get_connection(sftp_conn_id)
    hostname = connection.host
    username = connection.login
    key_file = connection.extra_dejson.get('key_file')  # Assuming the key file path is stored in the connection's extra field

    file_names = sftp_hook.list_directory(sftp_path)

    current_date = datetime.now()
    subfolder = current_date.strftime('%Y%m')
    day_stamp = current_date.strftime('%d')
    network_path_with_date = os.path.join(network_path, subfolder)

    if not os.path.exists(network_path_with_date):
        os.makedirs(network_path_with_date)

    if sftp_hook.isfile(os.path.join(sftp_path, file_name)):
        local_file_path = os.path.join(network_path_with_date, f'{file_name}_{day_stamp}')
        
        # Retrieve the home directory of the airflow_prod user via SSH
        home_dir_command = "echo ~airflow_prod"
        home_directory, error = execute_ssh_command(hostname, username, key_file, home_dir_command)
        if error:
            logger.error(f'Error retrieving home directory: {error}')
            return
        temp_local_file_path = os.path.join(home_directory, f'{file_name}_{day_stamp}')
        
        # Command to copy the file using sudo cp on the remote SFTP machine
        remote_cp_command = f"sudo cp {sftp_path}/{file_name} {temp_local_file_path}"
        logger.info(f'Executing command: {remote_cp_command}')
        output, error = execute_ssh_command(hostname, username, key_file, remote_cp_command)
        logger.info(f'Output: {output}')
        if error:
            logger.error(f'Error: {error}')
        logger.info(f'Copied {sftp_path}/{file_name} to {temp_local_file_path} on remote SFTP machine using sudo cp command')

        # Get the username from the SFTP connection ID
        sftp_username = get_sftp_username(sftp_conn_id)
        
        # Change the owner of the file to the SFTP connection user
        chown_command = f"sudo chown {sftp_username}:{sftp_username} {temp_local_file_path}"
        logger.info(f'Executing command: {chown_command}')
        output, error = execute_ssh_command(hostname, username, key_file, chown_command)
        logger.info(f'Output: {output}')
        if error:
            logger.error(f'Error: {error}')
        logger.info(f'Changed owner of {temp_local_file_path} to {sftp_username}')

        # Download the file from the remote SFTP location to the home directory
        sftp_hook.retrieve_file(os.path.join(sftp_path, file_name), temp_local_file_path)
        logger.info(f'Downloaded {file_name} from {sftp_path} to {temp_local_file_path}')

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
