from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import zipfile
import io
import os
import logging

DESTINATION_DIR = '/source-biakonzasftp/C-174/'

def process_zip_file(sftp_hook, zip_file, logger):
    sftp_client = sftp_hook.get_conn()
    try:
        with sftp_client.open(zip_file, 'rb') as remote_file:
            zip_bytes = remote_file.read()

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for file_name in zf.namelist():
                if not file_name.endswith('/'):
                    with zf.open(file_name) as f:
                        content = f.read()
                        output_path = os.path.join(DESTINATION_DIR, file_name)
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        with open(output_path, 'wb') as out_file:
                            out_file.write(content)
                        logger.info(f"Saved {file_name} from {zip_file} to {output_path}")

        sftp_client.remove(zip_file)
        logger.info(f"Deleted ZIP file from SFTP: {zip_file}")

    except Exception as e:
        logger.error(f"Failed to process {zip_file}: {e}", exc_info=True)

def unzip_and_cleanup_sftp_zips_multithreaded():
    logger = logging.getLogger("airflow.task")
    sftp_hook = SFTPHook(ftp_conn_id='Availity_Diameter_Health__DH_Fusion_Production_SFTP')
    sftp_client = sftp_hook.get_conn()

    os.makedirs(DESTINATION_DIR, exist_ok=True)

    files = sftp_client.listdir('.')
    zip_files = [f for f in files if f.lower().endswith('.zip')]
    logger.info(f"Number of ZIP files from SFTP: {len(zip_files)}")
    with ThreadPoolExecutor(max_workers=4) as executor:
        for zip_file in zip_files:
            executor.submit(process_zip_file, sftp_hook, zip_file, logger)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Availity_sftp_unzip_to_directory_C174',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'Canary','Staging_in_Prod'],
) as dag:

    unzip_task = PythonOperator(
        task_id='unzip_and_cleanup_sftp_zips_multithreaded',
        python_callable=unzip_and_cleanup_sftp_zips_multithreaded,
    )
