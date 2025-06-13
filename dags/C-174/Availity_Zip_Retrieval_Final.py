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
ARCHIVE_DESTINATION = '/source-biakonzasftp/C-194/archive_C-174/'

def process_zip_file(zip_file, logger):
    try:
        sftp_hook = SFTPHook(ftp_conn_id='Availity_Diameter_Health__Files_Production_SFTP')
        sftp_client = sftp_hook.get_conn()

        # Read the ZIP file from the source
        with sftp_client.open(zip_file, 'rb') as remote_file:
            zip_bytes = remote_file.read()

        # Save a copy to the archive directory on the SFTP
        archive_path = f'{ARCHIVE_DESTINATION}{os.path.basename(zip_file)}'
        with sftp_client.open(archive_path, 'wb') as archive_file:
            archive_file.write(zip_bytes)
        logger.info("Archived ZIP file to: %s", archive_path)

        # Extract contents locally
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for file_name in zf.namelist():
                if not file_name.endswith('/'):
                    with zf.open(file_name) as f:
                        content = f.read()
                        output_path = os.path.join(DESTINATION_DIR, file_name)
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        with open(output_path, 'wb') as out_file:
                            out_file.write(content)
                        logger.info("Saved %s from %s to %s", file_name, zip_file, output_path)

        # Delete the original ZIP file from the source
        sftp_client.remove(zip_file)
        logger.info("Deleted original ZIP file from SFTP: %s", zip_file)

    except zipfile.BadZipFile:
        logger.warning("Corrupt ZIP file skipped: %s", zip_file)
    except Exception as e:
        logger.error("Failed to process %s: %s", zip_file, str(e), exc_info=True)


def unzip_and_cleanup_sftp_zips_multithreaded():
    logger = logging.getLogger("airflow.task")
    sftp_hook = SFTPHook(ftp_conn_id='Availity_Diameter_Health__Files_Production_SFTP')
    sftp_client = sftp_hook.get_conn()

    os.makedirs(DESTINATION_DIR, exist_ok=True)

    files = sftp_client.listdir('.')
    zip_files = [f for f in files if f.lower().endswith('.zip')]
    logger.info("Number of ZIP files from SFTP: %d", len(zip_files))

    with ThreadPoolExecutor(max_workers=4) as executor:
        for zip_file in zip_files:
            executor.submit(process_zip_file, zip_file, logger)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Availity_Zip_Retrieval_Final',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'Canary', 'Staging_in_Prod'],
) as dag:

    unzip_task = PythonOperator(
        task_id='unzip_and_cleanup_sftp_zips_multithreaded',
        python_callable=unzip_and_cleanup_sftp_zips_multithreaded,
    )
