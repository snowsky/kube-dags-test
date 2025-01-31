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
import re
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
    max_active_runs=1,
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
##S3_SUBFOLDER = 'HL7v3In/XCAIn_test/'
LOCAL_DIR = '/source-biakonzasftp/C-128/archive/XCAIn'
#LOCAL_DIR = '/data/biakonzasftp/C-128/archive/XCAIn_AA'

#s3://konzaandssigrouppipelines/HL7v3In/XCAIn_test/
@task(dag=dag)
def list_files_in_s3():
    hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
    files = hook.list_keys(bucket_name=BUCKET_NAME, prefix=S3_SUBFOLDER)[:
    logging.info(f'Files in S3: {files}')
    return files

@task(dag=dag)
def filter_xml_files(files):
    xml_files = [file for file in files if file.endswith('.xml')]
    logging.info(f'Filtered XML Files: {xml_files}')
    if not xml_files:
        logging.warning("No XML files found. Check S3 structure!")
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

def get_sftp_test():
    if ENV == 'Dev':
        sftp_conn_id = 'sftp_airflow'
    if ENV == 'Prod':
        sftp_conn_id = 'Availity_Diameter_Health__Files_Test_Environment'
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
    """
    Ensure 2-folder structure exists in SFTP.
    - Works for folder1 and folder2.
    - No renaming of folder names.
    - Creates folders sequentially.
    """
    parts = file_key.split('/')
    if len(parts) < 3:
        logging.error(f"Invalid file structure for {file_key}. Expected at least 3 parts.")
        return

    try:
        folder1 = parts[-4].split('=')[1]
        folder2 = parts[-3].split('=')[1]
    except IndexError:
        logging.error(f"Error extracting folder structure from {file_key}.")
        return

    logging.info(f"Extracted Folders: folder1='{folder1}', folder2='{folder2}'")

    sftp, transport = get_sftp()

    try:
        base_path = "C-128/C_128_test_delivery/XCAIn" if ENV == "Dev" else "inbound"

        # Construct directory paths
        level1 = f"{base_path}/{folder1}"
        level2 = f"{level1}/{folder2}"

        # Create directories sequentially
        for path in [level1, level2]:
            try:
                sftp.stat(path)  # Check if directory exists
                logging.info(f"Directory exists: {path}")
            except FileNotFoundError:
                logging.info(f"Creating missing folder: {path}")
                sftp.mkdir(path)

    except Exception as e:
        logging.error(f"Error ensuring directories exist: {e}")
    finally:
        sftp.close()
        transport.close()



def ensure_directories_exist_test(file_key):
    """
    Ensure 3-level folder structure exists in SFTP.
    - Works for 3 folders only (folder1, folder2, folder3)
    - No renaming of folder names
    - Creates folders sequentially
    """
    parts = file_key.split('/')
    if len(parts) < 4:
        logging.error(f"Invalid file structure for {file_key}. Expected at least 4 parts.")
        return

    try:
        folder1 = parts[-4].split('=')[1] + "_3F"  #  Add _3F suffix as a unique identifier
        folder2 = parts[-3].split('=')[1]
        folder3 = parts[-2].split('=')[1] if len(parts) > 4 and '=' in parts[-2] else None
    except IndexError:
        logging.error(f"Error extracting folder structure from {file_key}.")
        return

    logging.info(f"Extracted Folders: folder1='{folder1}', folder2='{folder2}', folder3='{folder3}'")

    sftp, transport = get_sftp_test()

    try:
        base_path = "C-128/C_128_test_delivery/XCAIn" if ENV == "Dev" else "inbound"
        level1 = f"{base_path}/{folder1}"
        level2 = f"{level1}/{folder2}"
        level3 = f"{level2}/{folder3}" if folder3 else None

        # Ensure level1 (Folder1) exists
        try:
            sftp.stat(level1)
            logging.info(f"Directory exists: {level1}")
        except FileNotFoundError:
            logging.info(f"Creating missing folder: {level1}")
            sftp.mkdir(level1)

        # Ensure level2 (Folder2) exists
        try:
            sftp.stat(level2)
            logging.info(f"Directory exists: {level2}")
        except FileNotFoundError:
            logging.info(f"Creating missing folder: {level2}")
            sftp.mkdir(level2)

        # Only create level3 (Folder3) **if the file path explicitly contains it**
        if folder3:
            try:
                sftp.stat(level3)
                logging.info(f"Directory exists: {level3}")
            except FileNotFoundError:
                logging.info(f"Creating missing folder: {level3}")
                sftp.mkdir(level3)
        else:
            logging.warning(f"Skipping Folder3 because it's not present in the file path.")

    except Exception as e:
        logging.error(f"Error ensuring directories exist: {e}")
    finally:
        sftp.close()
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

def transfer_file_to_sftp_test(file_key):
    logging.info(f'Starting transfer for: {file_key}')
    if not file_key.endswith('.xml'):
        logging.info(f'Skipping non-XML file: {file_key}')
        return

    parts = file_key.split('/')
    folder1 = parts[-4].split('=')[1] + "_3F"  # Add _3F suffix
    folder2 = parts[-3].split('=')[1]
    folder3 = parts[-2].split('=')[1] if len(parts) > 4 and '=' in parts[-2] else None
    file_name = parts[-1]

    sftp_path = f"C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}/{folder3}/{file_name}" if folder3 else f"C-128/C_128_test_delivery/XCAIn/{folder1}/{folder2}/{file_name}"
    
    logging.info(f'SFTP Path: {sftp_path}')
    sftp, transport = get_sftp_test()

    try:
        s3_hook = S3Hook(aws_conn_id='konzaandssigrouppipelines')
        s3_object = s3_hook.get_key(file_key, BUCKET_NAME)

        with s3_object.get()['Body'] as s3_file:
            try:
                sftp.putfo(s3_file, sftp_path)
                logging.info(f'Transferred file: {file_name} to {sftp_path}')
            except IOError as e:
                if "No such file" in str(e) or e.errno == 2:  
                    logging.warning(f"Directory missing. Creating directories and retrying...")
                    ensure_directories_exist_test(file_key)  # Create folders
                    with s3_object.get()['Body'] as retry_s3_file:
                        sftp.putfo(retry_s3_file, sftp_path)  # Retry transfer
                    logging.info(f'Retry successful: Transferred {file_name}')
                else:
                    logging.error(f"Unexpected error during transfer: {e}")
                    raise
    except Exception as e:
        logging.error(f'Error during file transfer: {e}')
    finally:
        sftp.close()
        transport.close()


@task(dag=dag)
def transfer_batch_to_sftp_2_folder(batch: List[str]):
    for file_key in batch:
        parts = file_key.split('/')
        
        is_two_folder_structure = len(parts) > 3 and '=' in parts[-3]

        if is_two_folder_structure:
            logging.info(f"Detected **2-folder** structure for: {file_key}")
            ensure_directories_exist(file_key)  
            transfer_file_to_sftp(file_key)  
        else:
            logging.warning(f"File does NOT belong to 2-folder structure, skipping: {file_key}")

@task(dag=dag)
def transfer_batch_to_sftp_3_folder(batch: List[str]):
    for file_key in batch:
        parts = file_key.split('/')
        
        is_three_folder_structure = len(parts) > 4 and '=' in parts[-2]

        if is_three_folder_structure:
            logging.info(f"Detected **3-folder** structure for: {file_key}")

            ensure_directories_exist_test(file_key)  # This now checks if Folder3 is needed

            transfer_file_to_sftp_test(file_key)  
        else:
            logging.warning(f"File does NOT belong to 3-folder structure, skipping: {file_key}")



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
#transfer_tasks = transfer_batch_to_sftp.expand(batch=batches)
download_files = download_files_to_local(xml_files, local_dir=LOCAL_DIR, aws_conn_id="konzaandssigrouppipelines", bucket_name=BUCKET_NAME, max_workers="{{ params.max_workers }}")
transfer_tasks_2_folder = transfer_batch_to_sftp_2_folder.expand(batch=batches)
transfer_tasks_3_folder = transfer_batch_to_sftp_3_folder.expand(batch=batches)
delete_files = delete_files_from_s3(xml_files, aws_conn_id="konzaandssigrouppipelines", bucket_name=BUCKET_NAME)

files >> xml_files >> batches >> transfer_tasks_2_folder >> transfer_tasks_3_folder >> download_files >> delete_files
#files >> xml_files >> batches >> transfer_tasks_2_folder >> transfer_tasks_3_folder >> download_files 

if __name__ == "__main__":
    dag.cli()