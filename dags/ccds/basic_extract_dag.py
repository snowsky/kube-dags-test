from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from konza.parser import extract_demographic_info_from_xmls_to_parquet
import pandas as pd
import tempfile
import os
import logging

def test_basic_extract():
    with tempfile.TemporaryDirectory() as td:
        output_path = os.path.join(td, "output.parquet")
        extract_demographic_info_from_xmls_to_parquet("/opt/airflow/ccda/", output_path)
        data = pd.read_parquet(output_path)
        logging.info(data.to_string())
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 20),
    'retries': 0,
}

dag = DAG(
    'test_basic_extract',
    default_args=default_args,
    description='test basic extract',
    schedule_interval=None,
    catchup=False,
)

test_basic_extract_task = PythonOperator(
    task_id='test_basic_extract_dag',
    python_callable=test_basic_extract,
    dag=dag,
)

test_basic_extract_task

