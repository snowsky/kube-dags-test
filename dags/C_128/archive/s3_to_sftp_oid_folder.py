from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import paramiko
import json
from airflow.operators.python import PythonOperator
import logging
import re
from datetime import datetime
from typing import List
import boto3
#logging.basicConfig(level=logging.DEBUG)
# Define the DAG
default_args = {
    'owner': 'airflow',
}
dag = DAG(
    'XCAIn_s3_to_sftp_with_oid_folder',
    default_args=default_args,
    description='Retrieve files from S3 and deliver to SFTP with OID folder structure implemenmted',
    schedule=None,
    tags=['C-128'],
    catchup=False
)
ENV = 'Prod'
BUCKET_NAME = 'konzaandssigrouppipelines'
S3_SUBFOLDER = 'XCAIn/'

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

def get_sftp():
    if ENV == 'Dev':
        sftp_conn_id = 'sftp_airflow'
    if ENV == 'Prod':
        sftp_conn_id = 'Availity_Diameter_Health__DH_Fusion_Production_SFTP'
    sftp_conn = BaseHook.get_connection(sftp_conn_id)
    transport = paramiko.Transport((sftp_conn.host, sftp_conn.port))
    extra = json.loads(sftp_conn.extra)
        # if key_file in extra connect to ssh 
    if "key_file" in extra: 
        with open(extra["key_file"]) as f:
            pkey = paramiko.RSAKey.from_private_key(f)
        transport.connect(username=sftp_conn.login, pkey=pkey)
    else: 
        transport.connect(username=sftp_conn.login, password=sftp_conn.password)
    ## AA Edits :  commenting bogdons' code 
    #return paramiko.SFTPClient.from_transport(transport)
    ## AA Edits : To resolve NameError: name 'transport' is not defined
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

## @task(dag=dag)
def ensure_directories_exist(file_key):
    parts = file_key.split('/')
    folder1 = parts[-4].split('=')[1]  # First folder
    folder2 = parts[-3].split('=')[1]  # Second folder
    #folder1 = parts[-3]  # First folder
    #folder2 = parts[-2]  # Second folder
    ## AA Edits :  commenting bogdons' code 
    #sftp = get_sftp()
    ## AA Edits : To resolve NameError: name 'transport' is not defined
    sftp, transport = get_sftp()
    logging.debug(f'sftp type: {type(sftp)}, transport type: {type(transport)}')

    try:
        if ENV == 'Dev':
            try:
                sftp.chdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}') #changed sftp path as per Eric's screenshot in pr 105
            except IOError:
                sftp.mkdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}') #changed sftp path as per Eric's screenshot in pr 105
            try:
                sftp.chdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}') #changed sftp path as per Eric's screenshot in pr 105
            except IOError:
                sftp.mkdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}') #changed sftp path as per Eric's screenshot in pr 105
        if ENV == 'Prod':
            try:
                sftp.chdir(f'inbound/{folder1}') #changed sftp path as per Eric's screenshot in pr 105
            except IOError:
                sftp.mkdir(f'inbound/{folder1}') #changed sftp path as per Eric's screenshot in pr 105
            try:
                sftp.chdir(f'inbound/{folder1}/{folder2}') #changed sftp path as per Eric's screenshot in pr 105
            except IOError:
                sftp.mkdir(f'inbound/{folder1}/{folder2}') #changed sftp path as per Eric's screenshot in pr 105
    except Exception as e:
        logging.error(f'Error ensuring directories exist: {e}')
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

##@task(dag=dag)
def transfer_file_to_sftp(file_key):
    logging.info(f'Starting transfer for: {file_key}')
    if not file_key.endswith('.xml'):
        logging.info(f'Skipping non-XML file: {file_key}')
        return

    # Extract folder structure from the file_key
    parts = file_key.split('/')
    folder1 = parts[-4].split('=')[1]  # First folder
    folder2 = parts[-3].split('=')[1]  # Second folder
    #folder1 = parts[-3]  # First folder
    #folder2 = parts[-2]  # Second folder
    file_name = parts[-1]  # File name

    # Construct the SFTP path
    # Get SFTP connection details
    if ENV == 'Dev':
        sftp_path = f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}/{file_name}'  #changed sftp path as per Eric's screenshot in pr 105
    if ENV == 'Prod':
        sftp_path = f'inbound/{folder1}/{folder2}/{file_name}'  #changed sftp path as per Eric's screenshot in pr 105
    logging.info(f'SFTP Path: {sftp_path}')
    ## AA Edits :  commenting bogdons' code 
    #sftp = get_sftp()
    ## AA EDITs : adding statement to ensure  : unpack the tuple correctly for error : AttributeError: 'tuple' object has no attribute 'close'
    sftp, transport = get_sftp()  # Unpack the tuple returned by get_sftp
    try:
        # Establish SFTP connection
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
        ## AA EDITs : adding statement to ensure  : unpack the tuple correctly for error : AttributeError: 'tuple' object has no attribute 'close'
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


# Define the workflow
files = list_files_in_s3()
xml_files = filter_xml_files(files)

# Ensure directories exist for each file's folder structure
#ensure_directories_exist.expand(file_key=xml_files)
batches = divide_files_into_batches(xml_files)

# Transfer files
#transfer_tasks = transfer_file_to_sftp.expand(file_key=xml_files)
transfer_tasks = transfer_batch_to_sftp.expand(batch=batches)

if __name__ == "__main__":
    dag.cli()
