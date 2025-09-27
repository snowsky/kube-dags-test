import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Add the dags directory to Python path so we can import from lib.konza
dags_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, dags_dir)

from lib.konza.parser import extract_demographic_info_from_xmls_to_parquet
import pandas as pd
import tempfile
import logging

def basic_extract_print():
    with tempfile.TemporaryDirectory() as td:
        output_path = os.path.join(td, "output.parquet")
        extract_demographic_info_from_xmls_to_parquet("/opt/airflow/ccda/", output_path)
        data = pd.read_parquet(output_path)
        logging.info(data.to_string())
        return data

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
    python_callable=basic_extract_print,
    dag=dag,
)

test_basic_extract_task

