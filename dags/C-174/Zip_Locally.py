from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import zipfile
import os
import logging

# Configuration
SOURCE_DIR = '/source-biakonzasftp/C-194/archive_C-174'  # Root directory with dated subfolders
DESTINATION_DIR = '/source-biakonzasftp/C-194/restore_C-174'
BATCH_SIZE = 100000

def batch_zip_local_files():
    logger = logging.getLogger("airflow.task")

    os.makedirs(DESTINATION_DIR, exist_ok=True)

    # Collect all non-zip files from all subdirectories
    regular_files = []
    for root, dirs, files in os.walk(SOURCE_DIR):
        for file in files:
            if not file.lower().endswith('.zip'):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, SOURCE_DIR)
                regular_files.append((full_path, relative_path))

    logger.info("Total files to process: %d", len(regular_files))

    def zip_batch(batch_files, batch_index):
        zip_path = os.path.join(DESTINATION_DIR, f'batch_{batch_index + 1}.zip')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for full_path, relative_path in batch_files:
                try:
                    with open(full_path, 'rb') as local_file:
                        content = local_file.read()
                        zipf.writestr(relative_path, content)
                    logger.info("Added %s to %s", relative_path, zip_path)
                except Exception as e:
                    logger.error("Failed to add %s: %s", relative_path, str(e), exc_info=True)

    # Split into batches
    batches = [regular_files[i:i + BATCH_SIZE] for i in range(0, len(regular_files), BATCH_SIZE)]

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        for idx, batch in enumerate(batches):
            executor.submit(zip_batch, batch, idx)

# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG(
    dag_id='Zip_Dated_Subfolders_Locally',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['C-174', 'C-194', 'Batch_Zip', 'Local', 'Recursive'],
) as dag:

    zip_task = PythonOperator(
        task_id='batch_zip_local_files',
        python_callable=batch_zip_local_files,
    )
