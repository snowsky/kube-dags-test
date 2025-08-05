from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import zipfile
import io
import os
import logging

SOURCE_DIR = '/source-biakonzasftp/C-194/archive_C-174'  # Root directory on SFTP
DESTINATION_DIR = '/source-biakonzasftp/C-194/restore_C-174'
BATCH_SIZE = 100000

def batch_zip_sftp_files():
    logger = logging.getLogger("airflow.task")
    sftp_hook = SFTPHook(ftp_conn_id='Availity_Diameter_Health__Files_Production_SFTP')
    sftp_client = sftp_hook.get_conn()

    os.makedirs(DESTINATION_DIR, exist_ok=True)

    all_files = sftp_client.listdir(SOURCE_DIR)
    regular_files = [f for f in all_files if not f.lower().endswith('.zip')]

    logger.info("Total files to process: %d", len(regular_files))

    def zip_batch(batch_files, batch_index):
        zip_path = os.path.join(DESTINATION_DIR, f'batch_{batch_index + 1}.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_name in batch_files:
                try:
                    with sftp_client.open(file_name, 'rb') as remote_file:
                        content = remote_file.read()
                        zipf.writestr(file_name, content)
                    logger.info("Added %s to %s", file_name, zip_path)
                except Exception as e:
                    logger.error("Failed to add %s: %s", file_name, str(e), exc_info=True)

    batches = [regular_files[i:i + BATCH_SIZE] for i in range(0, len(regular_files), BATCH_SIZE)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        for idx, batch in enumerate(batches):
            executor.submit(zip_batch, batch, idx)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Zip_Locally_batch_zip_sftp_files_C174',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'C-194', 'Batch_Zip', 'SFTP'],
) as dag:

    zip_task = PythonOperator(
        task_id='batch_zip_sftp_files',
        python_callable=batch_zip_sftp_files,
    )
