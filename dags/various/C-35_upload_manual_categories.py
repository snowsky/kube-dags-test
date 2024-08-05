from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'example_wasb_dag',
    default_args=default_args,
    schedule_interval=None,
)

def list_blobs():
    hook = WasbHook(wasb_conn_id='hdinsightstorageaccount-blob-core-windows-net')
    blobs = hook.get_blobs_list(container_name='ertacontainer-prd51')
    for blob in blobs:
        logging.info(print(blob.name))
        print(blob.name)

list_blobs_task = PythonOperator(
    task_id='list_blobs',
    python_callable=list_blobs,
    dag=dag,
)
