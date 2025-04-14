from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

import pandas as pd
from pathlib import Path
import os
import logging

SOURCE_DIRECTORY = "/data/reportwriterstorage/mpi/"
DESTINATION_DIRECTORY = "/data/reportwriterstorage/mpi/Adjusted/"
WORKSHEET_FILE_ACCID_REF = "/data/reportwriterstorage/mpi_worksheets/SUP_11438_accid_ref_to_remove.csv"
SUB_DIRECTORIES = ["2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08", "2023-09", "2023-10", "2023-11", "2023-12", "2024-01", "2024-02", "2024-03", "2024-04", "2024-05"]
PARALLEL_TASK_LIMIT = 5

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'SLA-113process_parquet_files',
    default_args=default_args,
    schedule=None,
    tags=['example', 'sla113'],
    concurrency=PARALLEL_TASK_LIMIT,
) as dag:
    @task
    def generate_directory_list_task():
        dirs=[]
        for suffix in SUB_DIRECTORIES:
            source_sub_directory = os.path.join(SOURCE_DIRECTORY, suffix)
            destination_sub_directory = os.path.join(DESTINATION_DIRECTORY, suffix)
            dirs.append((source_sub_directory, destination_sub_directory))
        return dirs

    # Task to read the CSV
    def process_parquet_file(input_file_path, output_directory):
        os.makedirs(output_directory, exist_ok=True)
        accidDF = pd.read_csv(WORKSHEET_FILE_ACCID_REF)
        accidDF.columns = ['accid_ref']
        
        df = pd.read_parquet(input_file_path)
        if df.empty:
            logging.info(f'input file empty: f{input_file_path}')
            return
        dfnew = df[~df['accid_ref'].isin(accidDF['accid_ref'])]
        rows_removed = len(df.index) - len(dfnew.index)
        logging.info(f'{rows_removed} rows removed from: f{input_file_path}')
        output_file_path = os.path.join(output_directory, Path(input_file_path).name) 
        dfnew.to_parquet(output_file_path)
   
    @task(map_index_template="{{ input_directory }}")
    def process_parquet_files_in_directory(in_out_dirs):
        input_directory, output_directory = in_out_dirs
        context = get_current_context()
        context["input_directory"] = input_directory
        for file_name in os.listdir(input_directory):
            file_path = os.path.join(input_directory, file_name)
            process_parquet_file(file_path, output_directory)

    files_in_directory_process = process_parquet_files_in_directory.expand(in_out_dirs=generate_directory_list_task())
