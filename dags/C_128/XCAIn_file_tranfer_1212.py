from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import paramiko
import json
import logging
from datetime import datetime
from typing import List
from concurrent.futures import ThreadPoolExecutor as PoolExecutor, as_completed
from functools import partial
import os
from airflow.models.param import Param


# Define the DAG
default_args = {
    'owner': 'airflow',
}
dag = DAG(
    'XCAIn_s3_to_sftp_with_oid_folder_copy_to_archive',
    default_args=default_args,
    description='Retrieve files from S3 and deliver to SFTP with OID folder structure implemented and delivered to archive folder and delete from s3 after transfer(s)',
    schedule_interval='@hourly',
    start_date=datetime(2024, 12, 13), 
    tags=['C-128'],
    catchup=False,
    params={
        "max_workers": Param(5, type="integer", minimum=1),
        "batch_size": Param(100, type="integer", minimum=1)
    }
)
ENV = 'Prod'
BUCKET_NAME = 'konzaandssigrouppipelines'
S3_SUBFOLDER = 'XCAIn/'
LOCAL_DIR = '/source-biakonzasftp/C-128/archive/XCAIn'
#s3://konzaandssigrouppipelines/HL7v3In/XCAIn_test/
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
    if "key_file" in extra: 
        with open(extra["key_file"]) as f:
            pkey = paramiko.RSAKey.from_private_key(f)
        transport.connect(username=sftp_conn.login, pkey=pkey)
    else: 
        transport.connect(username=sftp_conn.login, password=sftp_conn.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport

def ensure_directories_exist(file_key):
    parts = file_key.split('/')
    folder1 = parts[-4].split('=')[1]
    folder2 = parts[-3].split('=')[1]
    sftp, transport = get_sftp()
    logging.debug(f'sftp type: {type(sftp)}, transport type: {type(transport)}')

    try:
        if ENV == 'Dev':
            try:
                sftp.chdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}')
            except IOError:
                sftp.mkdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}')
            try:
                sftp.chdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}')
            except IOError:
                sftp.mkdir(f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}')
        if ENV == 'Prod':
            try:
                sftp.chdir(f'inbound/{folder1}')
            except IOError:
                sftp.mkdir(f'inbound/{folder1}')
            try:
                sftp.chdir(f'inbound/{folder1}/{folder2}')
            except IOError:
                sftp.mkdir(f'inbound/{folder1}/{folder2}')
    except Exception as e:
        logging.error(f'Error ensuring directories exist: {e}')
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def transfer_file_to_sftp(file_key):
    logging.info(f'Starting transfer for: {file_key}')
    if not file_key.endswith('.xml'):
        logging.info(f'Skipping non-XML file: {file_key}')
        return

    parts = file_key.split('/')
    folder1 = parts[-4].split('=')[1]
    folder2 = parts[-3].split('=')[1]
    file_name = parts[-1]

    if ENV == 'Dev':
        sftp_path = f'C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}/{file_name}'
    if ENV == 'Prod':
        sftp_path = f'inbound/{folder1}/{folder2}/{file_name}'
    logging.info(f'SFTP Path: {sftp_path}')
    sftp, transport = get_sftp()
    try:
        s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
        s3_object = s3_hook.get_key(file_key, BUCKET_NAME)

        with s3_object.get()['Body'] as s3_file:
            sftp.putfo(s3_file, sftp_path)
            logging.info(f'Transferred file: {file_name} to {sftp_path}')
        return file_key
    except Exception as e:
        logging.error(f'Error during file transfer: {e}')
        return None
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

@task(dag=dag)
def transfer_batch_to_sftp(batch: List[str]):
    for file_key in batch:
        ensure_directories_exist(file_key)
        transfer_file_to_sftp(file_key)

@task(dag=dag)
def divide_files_into_batches(xml_files: List[str], batch_size: str) -> List[List[str]]:
    batch_size = int(batch_size)  # Convert batch_size to integer
    return [
        xml_files[i: i + batch_size] 
        for i in range(0, len(xml_files), batch_size)
    ]

def _download_file_from_s3(local_dir, aws_conn_id, bucket_name, file_key):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    # Parse file
    parts = file_key.split('/')
    folder1 = parts[-4].split('=')[1]
    folder2 = parts[-3].split('=')[1]
    file_name = parts[-1]
    
    # Create date folder
    date_folder = datetime.now().strftime('%Y-%m-%d')
    # Create same oid folder pattern as in client SFTP location
    local_path = f"{local_dir}/{date_folder}/{folder1}/{folder2}/{file_name}"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    s3_hook.download_file(
        key=file_key,
        bucket_name=bucket_name,
        local_path=local_path,
        preserve_file_name=True,
        use_autogenerated_subdir=False
    )
    return file_key

@task(dag=dag)
def download_files_to_local(xml_files, local_dir, aws_conn_id, bucket_name, max_workers: str):
    max_workers = int(max_workers)  # Convert max_workers to integer
    with PoolExecutor(max_workers=max_workers) as executor:
        future_file_dict = {executor.submit(partial(_download_file_from_s3, local_dir, aws_conn_id, bucket_name), file_key): file_key for file_key in xml_files}
        for future in as_completed(future_file_dict):
            file_key = future_file_dict[future]
            try:
                future.result()
                logging.info(f"Downloaded {file_key} to {local_dir}")
            except Exception as e:
                logging.error(f"Failed to download {file_key}: {e}")

@task(dag=dag)
def delete_files_from_s3(xml_files, aws_conn_id, bucket_name):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_client = s3_hook.get_conn()

    # Step 1: Delete XML files
    for file_key in xml_files:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            logging.info(f"Deleted file: {file_key}")
        except Exception as e:
            logging.error(f"Failed to delete file {file_key}: {e}")

    # Step 2: Delete empty directories, skipping the S3_SUBFOLDER
    deleted_directories = set()

    for file_key in xml_files:
        try:
            # Extract base directory path
            base_directory_path = '/'.join(file_key.split('/')[:-1]) + '/'
            if base_directory_path == S3_SUBFOLDER:
                continue  # Skip the root folder (S3_SUBFOLDER)

            logging.info(f"Checking directory: {base_directory_path}")
            
            # List objects within the directory
            objects_in_dir = s3_hook.list_keys(bucket_name=bucket_name, prefix=base_directory_path)
            if not objects_in_dir:
                # Directory is empty, delete it
                s3_client.delete_object(Bucket=bucket_name, Key=base_directory_path)
                logging.info(f"Deleted empty directory: {base_directory_path}")
                deleted_directories.add(base_directory_path)
            
            # Check parent directories recursively
            parts = base_directory_path.split('/')
            for i in range(len(parts) - 1, 0, -1):
                dir_path = '/'.join(parts[:i]) + '/'
                if dir_path == S3_SUBFOLDER or dir_path in deleted_directories:
                    continue  # Skip root folder or already deleted directories

                # Check if the parent directory is empty
                objects_in_parent_dir = s3_hook.list_keys(bucket_name=bucket_name, prefix=dir_path)
                if not objects_in_parent_dir:
                    s3_client.delete_object(Bucket=bucket_name, Key=dir_path)
                    logging.info(f"Deleted empty parent directory: {dir_path}")
                    deleted_directories.add(dir_path)
        except Exception as e:
            logging.error(f"Failed to delete directory {file_key}: {e}")


# Define the workflow
files = list_files_in_s3()
xml_files = filter_xml_files(files)
batches = divide_files_into_batches(xml_files, batch_size="{{ params.batch_size }}")
transfer_tasks = transfer_batch_to_sftp.expand(batch=batches)
download_files = download_files_to_local(xml_files, local_dir=LOCAL_DIR, aws_conn_id="konzaandssigrouppipelines", bucket_name=BUCKET_NAME, max_workers="{{ params.max_workers }}")
delete_files = delete_files_from_s3(xml_files, aws_conn_id="konzaandssigrouppipelines", bucket_name=BUCKET_NAME)

files >> xml_files >> batches >> transfer_tasks >> download_files >> delete_files

if __name__ == "__main__":
    dag.cli()
