from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import paramiko
from airflow.operators.python import PythonOperator
import logging
import re
from typing import List
from datetime import datetime
import boto3

# Define the DAG
default_args = {
    'owner': 'airflow',
}
dag = DAG(
    'HL7v3In_s3_to_sftp_with_oid_folder_delete_from_s3',
    default_args=default_args,
    description='Retrieve files from S3 and deliver to SFTP, delete from S3. For testing purpose, referencing S3_SUBFOLDER as HL7v3In(change done on 12/9)',
    schedule=None,
    tags=['C-128'],
    catchup=False
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
def filter_xml_files(files):
    xml_files = [file for file in files if file.endswith('.xml')]
    logging.info(f'Filtered XML Files: {xml_files}')
    return xml_files

##@task(dag=dag)
def ensure_directories_exist(file_key):
    parts = file_key.split('/')
    folder1 = parts[-3]  # First folder
    folder2 = parts[-2]  # Second folder

    sftp_conn_id = 'sftp_airflow'
    sftp_conn = BaseHook.get_connection(sftp_conn_id)

    transport = None
    sftp = None
    try:
        # Establish SFTP connection
        transport = paramiko.Transport((sftp_conn.host, sftp_conn.port))
        transport.connect(username=sftp_conn.login, password=sftp_conn.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Ensure the first folder exists
        try:
            sftp.chdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}') #changed sftp path as per Eric's screenshot in pr 105
        except IOError:
            sftp.mkdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}') #changed sftp path as per Eric's screenshot in pr 105
            logging.info(f'Created directory: {folder1}')

        # Ensure the second folder exists
        try:
            sftp.chdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}') #changed sftp path as per Eric's screenshot in pr 105
        except IOError:
            sftp.mkdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}') #changed sftp path as per Eric's screenshot in pr 105
            logging.info(f'Created directory: {folder2}')
    except Exception as e:
        logging.error(f'Error ensuring directories exist: {e}')
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
#@task(dag=dag)
def transfer_file_to_sftp(file_key):
    logging.info(f'Starting transfer for: {file_key}')
    if not file_key.endswith('.xml'):
        logging.info(f'Skipping non-XML file: {file_key}')
        return

    # Extract folder structure from the file_key
    parts = file_key.split('/')
    folder1 = parts[-3]  # First folder
    folder2 = parts[-2]  # Second folder
    file_name = parts[-1]  # File name

    # Construct the SFTP path
    sftp_path = f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}/{file_name}'  #changed sftp path as per Eric's screenshot in pr 105
    logging.info(f'SFTP Path: {sftp_path}')

    # Get SFTP connection details
    sftp_conn_id = 'sftp_airflow'
    sftp_conn = BaseHook.get_connection(sftp_conn_id)

    transport = None
    sftp = None
    try:
        # Establish SFTP connection
        transport = paramiko.Transport((sftp_conn.host, sftp_conn.port))
        transport.connect(username=sftp_conn.login, password=sftp_conn.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Stream the file directly from S3 to SFTP
        s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
        s3_object = s3_hook.get_key(file_key, BUCKET_NAME)

        # Upload the file to SFTP
        with s3_object.get()['Body'] as s3_file:
            sftp.putfo(s3_file, sftp_path)
            logging.info(f'Transferred file: {file_name} to {sftp_path}')
        return file_key  # Return the file key for deletion
    except Exception as e:
        logging.error(f'Error during file transfer: {e}')
        return None  # Return None if failed
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
            
@task(dag=dag)
def transfer_batch_to_sftp(batch: List[str]):
    for file_key in batch:
        # Ensure directories exist for each file's folder structure
        ensure_directories_exist(file_key)
        # Transfer files
        transfer_file_to_sftp(file_key)

@task(dag=dag)
def divide_files_into_batches(xml_files: List[str], batch_size=100) -> List[List[str]]:
    return [
        xml_files[i: i + batch_size] 
        for i in range(0, len(xml_files), batch_size)
    ]

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

# Define the delete task
delete_files_task = PythonOperator(
    task_id='delete_files',
    python_callable=delete_files_from_s3,
    provide_context=True,
    dag=dag,
)

# Define the workflow
files = list_files_in_s3()
xml_files = filter_xml_files(files)

# Ensure directories exist for each file's folder structure
#ensure_directories_exist.expand(file_key=xml_files)
batches = divide_files_into_batches(xml_files)

# Transfer files
#transfer_tasks = transfer_file_to_sftp.expand(file_key=xml_files)
transfer_tasks = transfer_batch_to_sftp.expand(batch=batches)

# Set dependencies
transfer_tasks >> delete_files_task  # Ensure delete runs after transfers

if __name__ == "__main__":
    dag.cli()
