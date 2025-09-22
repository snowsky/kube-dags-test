from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
from pathlib import Path

# Define the directories
restore_dir = Path("/source-biakonzasftp/C-194/restore_C-174/")
archive_dir = Path("/source-biakonzasftp/C-194/archive_C-174/20250916/")
remainder_dir = restore_dir / "remainder"
remainder_zip_path = remainder_dir / "remainder_files.zip"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='stream_restore_zip_remainder',
    default_args=default_args,
    schedule_interval=None,
    tags=['C-174'],
    catchup=False,
    description='Stream restore zip files and extract files not in archive',
) as dag:

    def get_archive_contents(**context):
        remainder_dir.mkdir(parents=True, exist_ok=True)
        contents = set()
        for zip_path in archive_dir.glob("*.zip"):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                contents.update(zip_ref.namelist())
        context['ti'].xcom_push(key='archive_contents', value=list(contents))

    def stream_unique_files(**context):
        archive_contents = set(context['ti'].xcom_pull(key='archive_contents'))
        restore_zip_files = list(restore_dir.glob("*.zip"))

        with zipfile.ZipFile(remainder_zip_path, 'w') as zip_out:
            for zip_path in restore_zip_files:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for file_name in zip_ref.namelist():
                        if file_name not in archive_contents:
                            with zip_ref.open(file_name) as source_file:
                                zip_out.writestr(file_name, source_file.read())

    task_get_archive_contents = PythonOperator(
        task_id='get_archive_contents',
        python_callable=get_archive_contents,
        execution_timeout=timedelta(minutes=5),
    )

    task_stream_unique_files = PythonOperator(
        task_id='stream_unique_files',
        python_callable=stream_unique_files,
        execution_timeout=timedelta(minutes=120),
    )

    task_get_archive_contents >> task_stream_unique_files
