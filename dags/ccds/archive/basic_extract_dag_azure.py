from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from lib.konza.parser import extract_demographic_info_from_xmls_to_parquet
import pandas as pd
import tempfile
import os
import logging
from random import choice
from string import ascii_uppercase
import py7zr

def test_basic_extract_azure():
    hook = AzureDataLakeStorageV2Hook(adls_conn_id='scienzrwstorage')
    container_name = 'content'
    directory = 'raw_ccds/20240314'
    file_sys = hook.get_file_system(file_system=container_name)
    file_paths = file_sys.get_paths(directory)

    with tempfile.TemporaryDirectory() as td:
        data_path = os.path.join(td, 'data/')
        extract_path = os.path.join(td, 'extract/')
        os.makedirs(data_path)
        os.makedirs(extract_path)
        for path in file_paths:
            file_client = file_sys.get_file_client(path)
            file_stream = file_client.download_file()
            file_bytes = file_stream.readall()
            data_file = os.path.join(data_path, ''.join(choice(ascii_uppercase) for i in range(8)) + '.bytes')
            with open(data_file, "wb") as data:
                data.write(file_bytes)
            with py7zr.SevenZipFile(data_file, mode="r") as z:
                z.extractall(path=extract_path)
        output_path = os.path.join(td, "output.parquet")
        extract_demographic_info_from_xmls_to_parquet(extract_path, output_path)
        data = pd.read_parquet(output_path)
        logging.info(data.to_string())
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 20),
    'retries': 0,
}

dag = DAG(
    'test_basic_extract_azure',
    default_args=default_args,
    description='test basic extract with data from azure storage account',
    schedule=None,
    catchup=False,
)

test_basic_extract_task_azure = PythonOperator(
    task_id='test_basic_extract_dag_azure',
    python_callable=test_basic_extract_azure,
    dag=dag,
)

test_basic_extract_task_azure

