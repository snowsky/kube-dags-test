from airflow import DAG
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import shutil

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 15),
}

dag = DAG(
    'aa_transfer_syslog_to_local_sftp_2',
    default_args=default_args,
    description='Retrieve files from SFTP, deliver to network path and S3',
    schedule_interval=None,
    catchup=False,
    tags=['S-6','AA', 'Test'],
)

# Define network file path
network_file_path = '/data/biakonzasftp/S-6/firewall_logs/'
file_name = 'syslog'

# Python function to copy files from SFTP to network file path
def copy_to_network_path(sftp_conn_id, sftp_path, network_path):
    sftp_hook = SFTPHook(sftp_conn_id)
    file_names = sftp_hook.list_directory(sftp_path)

    if not os.path.exists(network_path):
        os.makedirs(network_path)

    if sftp_hook.isfile(os.path.join(sftp_path, file_name)):
        local_file_path = os.path.join(network_path, file_name)
        sftp_hook.retrieve_file(os.path.join(sftp_path, file_name), local_file_path)
        print(f'Copied {file_name} to {network_path}')

# Task 1: Copy files from SFTP to network file path
copy_to_network_task = PythonOperator(
    task_id='copy_files_to_network',
    python_callable=copy_to_network_path,
    op_kwargs={
        'sftp_conn_id': 'PRD-AZ1-LOG2-airflow',
        'sftp_path': '/var/log/',
        'network_path': network_file_path,
    },
    dag=dag,
)