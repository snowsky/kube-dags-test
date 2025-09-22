from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import zipfile
from pathlib import Path

# Define the directories
restore_dir = Path("/data/biakonzasftp/C-194/restore_C-174/")
archive_dir = Path("/data/biakonzasftp/C-194/archive_C-174/20250916/")
remainder_dir = restore_dir / "remainder"
remainder_zip_path = remainder_dir / "remainder_files.zip"

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 22),
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='compare_restore_vs_archive_zips',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Compare restore zip files with archive and extract unique files',
) as dag:

    def get_archive_contents(**context):
        remainder_dir.mkdir(parents=True, exist_ok=True)
        contents = set()
        zip_files = list(archive_dir.glob("*.zip"))
        for zip_path in zip_files:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                contents.update(zip_ref.namelist())
        context['ti'].xcom_push(key='archive_contents', value=list(contents))

    def identify_unique_files(**context):
        archive_contents = set(context['ti'].xcom_pull(key='archive_contents'))
        restore_zip_files = list(restore_dir.glob("*.zip"))
        unique_files = {}

        for zip_path in restore_zip_files:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name not in archive_contents:
                        unique_files[file_name] = zip_ref.read(file_name)

        context['ti'].xcom_push(key='unique_files', value=unique_files)

    def write_remainder_zip(**context):
        unique_files = context['ti'].xcom_pull(key='unique_files')
        with zipfile.ZipFile(remainder_zip_path, 'w') as zip_out:
            for file_name, file_data in unique_files.items():
                zip_out.writestr(file_name, file_data)

    task_get_archive_contents = PythonOperator(
        task_id='get_archive_contents',
        python_callable=get_archive_contents,
        provide_context=True,
    )

    task_identify_unique_files = PythonOperator(
        task_id='identify_unique_files',
        python_callable=identify_unique_files,
        provide_context=True,
    )

    task_write_remainder_zip = PythonOperator(
        task_id='write_remainder_zip',
        python_callable=write_remainder_zip,
        provide_context=True,
    )

    task_get_archive_contents >> task_identify_unique_files >> task_write_remainder_zip
