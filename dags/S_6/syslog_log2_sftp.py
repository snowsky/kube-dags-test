from airflow import DAG
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import shutil
import pwd
import paramiko

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
}

dag = DAG(
    'prd-az1-log2_syslog_to_local_sftp',
    default_args=default_args,
    description='This DAG retrieves some firewall logs from a VM where they are collected and stores it for future audits with HITRUST implications',
    schedule_interval='@daily',  # Schedule to run daily
    catchup=False,
    tags=['S-6'],
)

# Define network file path
network_file_path = '/source-biakonzasftp/S-6/firewall_logs/'
file_name = 'syslog'

# Python function to copy files from SFTP to network file path
def copy_to_network_path(sftp_conn_id, sftp_path, network_path):
    sftp_hook = SFTPHook(sftp_conn_id)
    connection = sftp_hook.get_connection(sftp_conn_id)
    
    file_names = sftp_hook.list_directory(sftp_path)

    current_date = datetime.now()
    subfolder = current_date.strftime('%Y%m')
    day_stamp = current_date.strftime('%d')
    network_path_with_date = os.path.join(network_path, subfolder)

    if not os.path.exists(network_path_with_date):
        os.makedirs(network_path_with_date)

    if sftp_hook.isfile(os.path.join(sftp_path, file_name)):
        local_file_path = os.path.join(network_path_with_date, f'{file_name}_{day_stamp}')
        
        # Change ownership to the current user using paramiko
        current_user = pwd.getpwuid(os.getuid()).pw_name
        transport = paramiko.Transport((connection.host, connection.port))
        transport.connect(username=connection.login, password=connection.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.chown(os.path.join(sftp_path, file_name), os.getuid(), os.getgid())
        sftp.close()
        transport.close()
        
        sftp_hook.retrieve_file(os.path.join(sftp_path, file_name), local_file_path)
        print(f'Copied {file_name} to {network_path_with_date} with day stamp {day_stamp}')

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
)
