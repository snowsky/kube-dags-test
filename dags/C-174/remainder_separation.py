from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import zipfile
from pathlib import Path
import csv
import logging

# Define directories
restore_dir = Path("/source-biakonzasftp/C-194/restore_C-174/")
archive_base_dir = Path("/source-biakonzasftp/C-194/archive_C-174/")
exclude_dir = archive_base_dir / "20250916"
remainder_dir = restore_dir / "remainder"
remainder_zip_path = remainder_dir / "remainder_files.zip"
included_csv_path = remainder_dir / "included_files.csv"
excluded_csv_path = remainder_dir / "excluded_files.csv"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=120),
}

with DAG(
    dag_id='stream_restore_zip_remainder',
    default_args=default_args,
    schedule_interval=None,
    tags=['C-174'],
    catchup=False,
    description='Stream restore zip files and extract files not in latest archive',
) as dag:

    def get_exclude_contents(**context):
        remainder_dir.mkdir(parents=True, exist_ok=True)
        exclude_files = set()

        for zip_path in exclude_dir.glob("*.zip"):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                exclude_files.update(Path(name).name for name in zip_ref.namelist())

        # Push to XCom
        context['ti'].xcom_push(key='exclude_files', value=list(exclude_files))

        # Write excluded files to CSV
        with excluded_csv_path.open('w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Excluded Files'])
            for file in sorted(exclude_files):
                writer.writerow([file])

        logging.info(f"Total excluded files listed: {len(exclude_files)}")

    def stream_remainder_files(**context):
        exclude_files = set(context['ti'].xcom_pull(key='exclude_files'))
        restore_zip_files = list(restore_dir.glob("*.zip"))

        included_files = []
        excluded_count = 0
        with zipfile.ZipFile(remainder_zip_path, 'w') as zip_out:
            for zip_path in restore_zip_files:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for file_name in zip_ref.namelist():
                        base_name = Path(file_name).name
                        if base_name not in exclude_files:
                            with zip_ref.open(file_name) as source_file:
                                zip_out.writestr(file_name, source_file.read())
                            included_files.append(base_name)
                        else:
                            excluded_count += 1

        # Write included files to CSV
        with included_csv_path.open('w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Included Files'])
            for file in sorted(included_files):
                writer.writerow([file])

        logging.info(f"Total files included in remainder zip: {len(included_files)}")
        logging.info(f"Total files excluded: {excluded_count}")

    task_get_exclude_contents = PythonOperator(
        task_id='get_exclude_contents',
        python_callable=get_exclude_contents,
        execution_timeout=timedelta(minutes=5),
    )

    task_stream_remainder_files = PythonOperator(
        task_id='stream_remainder_files',
        python_callable=stream_remainder_files,
        execution_timeout=timedelta(minutes=120),
    )

    task_get_exclude_contents >> task_stream_remainder_files
