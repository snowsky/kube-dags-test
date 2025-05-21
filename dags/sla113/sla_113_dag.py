from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
from airflow.decorators import task
from airflow.models.param import Param

from sla113.sla_113_dag_impl import get_ids_to_delete_impl


from datetime import datetime
import pandas as pd
from pathlib import Path
import logging
import json
import os

test_env = Variable.get("TEST_ENV")
if test_env:
    from sla113.test_common import SOURCE_FILES_DIRECTORY, CONNECTION_NAME, TMP_OUTPUT_DIR, TARGET_TABLE
else:
    from sla113.common import SOURCE_FILES_DIRECTORY, CONNECTION_NAME, TMP_OUTPUT_DIR, TARGET_TABLE


MAX_DELETE_ROWS = None


class ReturningMySqlOperator(MySqlOperator):
    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = MySqlHook(mysql_conn_id=self.conn_id)
        return hook.get_records(
            self.sql,
            parameters=self.parameters
        )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    'SLA-113MySQL',
    default_args=default_args,
    schedule_interval=None,#'@daily'
    tags=['SLA-113', 'sla113'],
    params={
    "source_files_dir_path": Param(SOURCE_FILES_DIRECTORY, type="string"),
    "output_files_dir_path": Param(TMP_OUTPUT_DIR, type="string")
     },

) as dag:

    def find_files_to_process(source_files_dir):
        files_to_process = [
            file_path
            for file_path in Path(source_files_dir).glob("*")
            if file_path.name.find("processed") == -1
        ]
        logging.info(f"Found files to process: {files_to_process}")
        return files_to_process

    def get_ids_to_delete_from_file(input_file_path, max_id, output_file_path):

        def fetch_rows(start_id, end_id):
            mysql_hook = MySqlHook(mysql_conn_id=CONNECTION_NAME)
            sql = f"SELECT * FROM {TARGET_TABLE} WHERE id BETWEEN {start_id} AND {end_id}"
            df = mysql_hook.get_pandas_df(sql)
            return df

        def compare_and_get_ids(df1, df2):
            df2.columns=["accid_ref"]
            merged_df = pd.merge(df1, df2, on=['accid_ref'])  # Replace with your actual column names
            ids_to_delete = merged_df['id'].tolist()
            return ids_to_delete

        df2 = pd.read_csv(input_file_path)
        ids_to_delete = []
        chunk_size = 1000000
        id_range = range(0, max_id, chunk_size)

        for start_id in id_range:
            end_id = max_id if start_id == id_range[-1] else start_id + chunk_size -1
            df1 = fetch_rows(start_id, end_id)
            ids_to_delete.extend(compare_and_get_ids(df1,df2))
        if ids_to_delete:
            with open(output_file_path, "w") as f:
                json.dump(ids_to_delete, f)
            return output_file_path
        else: 
            logging.info("No ID's to delete")
            return None

    def merge_ids_to_delete_files(input_file_paths, output_file_path):
        ids_to_delete = []
        for input_file_path in input_file_paths:
            with open(input_file_path) as f:
                ids_to_delete += json.load(f)
        with open(output_file_path, "w") as f:
            json.dump(ids_to_delete,f)

    max_id = ReturningMySqlOperator(
        task_id='get_max_id',
        sql=f"SELECT MAX(id) FROM {TARGET_TABLE}",
        mysql_conn_id=CONNECTION_NAME
    )

    def delete_intermediate_files(input_file_paths):
        for input_file_path in input_file_paths:
            os.remove(input_file_path)

    
    @task
    def generate_ids_to_delete_file(max_id_output, params: dict):
        output_files_dir = params['output_files_dir_path']

        def generate_random_file_name(dir):
            return os.path.join(dir, f"merge_file_{randint(100000, 999999)}.ids")

        from random import randint

        paths_to_files_containing_ids_to_delete = []
        files_to_process = find_files_to_process(params['source_files_dir_path'])
        for input_file_path in files_to_process:
            output_file_path = os.path.join(output_files_dir, f"{input_file_path.name}.ids")
            path_to_found_ids_to_delete_in_this_file = get_ids_to_delete_from_file(input_file_path, max_id_output[0][0], output_file_path)
            if path_to_found_ids_to_delete_in_this_file:
                paths_to_files_containing_ids_to_delete.append(path_to_found_ids_to_delete_in_this_file)

        if paths_to_files_containing_ids_to_delete:
            merge_file_path = generate_random_file_name(output_files_dir)
            while os.path.isfile(merge_file_path):
                merge_file_path = generate_random_file_name(output_files_dir)
            merge_ids_to_delete_files(paths_to_files_containing_ids_to_delete, merge_file_path)
            delete_intermediate_files(paths_to_files_containing_ids_to_delete)
            return merge_file_path

        else:
            logging.info("No files containing id's to delete found" )
            return None


    @task
    def get_ids_to_delete(ids_to_delete_file):
        return get_ids_to_delete_impl(ids_to_delete_file, MAX_DELETE_ROWS)

    @task
    def delete_ids_from_tbl(ids_to_delete):
        def delete_ids_in_batches(ids_to_delete, batch_size=500):
            for i in range(0, len(ids_to_delete), batch_size):
                batch = ids_to_delete[i:i + batch_size]
                delete_statement = f"DELETE FROM {TARGET_TABLE} WHERE id IN ({','.join(map(str, batch))})"
                mysql_hook = MySqlHook(mysql_conn_id=CONNECTION_NAME)
                mysql_hook.run(delete_statement)
                logging.info(f'Deleted ids: {batch}')
        delete_ids_in_batches(ids_to_delete)
        logging.info(f'Deleted ids: {ids_to_delete}')

    generate_ids_to_delete_file_task = generate_ids_to_delete_file(max_id_output=max_id.output, params=dag.params)
    get_ids_to_delete_task = get_ids_to_delete(generate_ids_to_delete_file_task)
    delete_ids_from_tbl_task = delete_ids_from_tbl.expand(ids_to_delete=get_ids_to_delete_task)

    # Set task dependencies
    max_id >> generate_ids_to_delete_file_task >> get_ids_to_delete_task >> delete_ids_from_tbl_task


if __name__ == "__main__":
    dag.cli()
