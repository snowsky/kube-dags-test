from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess
import json
import logging

def print_licenses():
    result = subprocess.run(['pip-licenses', '--format=json'], capture_output=True, text=True)
    licenses = json.loads(result.stdout)
    
    for pkg in licenses:
        logging.info(f"Package: {pkg['Name']}, License: {pkg['License']}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'print_licenses_dag',
    default_args=default_args,
    tags=['testing'],
    description='A simple DAG to print package licenses',
    schedule=timedelta(days=1),
)

print_licenses_task = PythonOperator(
    task_id='print_licenses',
    python_callable=print_licenses,
    dag=dag,
)

print_licenses_task
