from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import logging
#assert False
MYSQL_CONN = 'qa-az1-sqlw2-airflowconnection'
SOURCE_FILES_DIRECTORY = '/source-biakonzasftp/C-9/SLA-113/'
TARGET_TABLE = 'person_master._mpi'
default_args = {
    'owner': 'airflow',
    'working_dir': SOURCE_FILES_DIRECTORY,
    'mysql_conn_id': MYSQL_CONN,
    'output_file_path': 
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'SLA-113MySQL',
    default_args=default_args,
    schedule=None,#'@daily'
    tags=['example'],
) as dag:
    def find_files_to_process():
        return [
            file_path
            for file_path in Path(SOURCE_FILES_DIRECTORY).glob("*")
            if file_path.name.find("processed") == -1
        ]

    @task
    def get_ids_to_delete_from_file(input_file_path, max_id, output_file_path):

        def fetch_rows(start_id, end_id):
            mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN)
            sql = f"SELECT * FROM {TARGET_TABLE} WHERE id BETWEEN {start_id} AND {end_id}"
            df = mysql_hook.get_pandas_df(sql)
            return df

        def compare_and_get_ids(df1, df2):
            merged_df = pd.merge(df1, df2, on=['accid_ref', 'accid_ref'])  # Replace with your actual column names
            ids_to_delete = merged_df['id'].tolist()
            return ids_to_delete

        df2 = pd.read_csv(input_file_path)
        ids_to_delete = []
        for start_id in range(0, max_id, chunk_size):
            end_id = min([start_id + chunk_size - 1, max_id])
            df1 = fetch_rows(start_id, end_id)
            ids_to_delete.append(compare_and_get_ids(df1, df2))

        with open(output_file_path, "w") as f:
            json.dump(f, ids_to_delete)
        return output_file_path

    @task
    def merge_ids_to_delete_files(input_file_paths, output_file_path):
        ids_to_delete = []
        for input_file_path in input_file_paths:
            with open(input_file_path) as f:
                ids_to_delete += json.load(f)
        with open(output_file_path, "w") as f:
            json.dump(f, ids_to_delete)
        return output_file_path

    max_id = MySqlOperator(
        task_id='get_max_id',
        sql="SELECT MAX(id) FROM {TARGET_TABLE}",
        mysql_conn_id=MYSQL_CONN
    )
    path_to_files_containing_ids_to_delete = []
    files_to_process = find_files_to_process()
    for input_file_path in files_to_process:
         output_file_path = os.path.join(TMP_OUTPUT_DIR_ON_DATA, f"{input_file_path.name}.ids")
         path_to_found_ids_to_delete_in_this_file = get_ids_to_delete_from_file(input_file_path, max_id, output_file_path)
         path_to_files_containing_ids_to_delete.append(path_to_found_ids_to_delete_in_this_file)
    path_to_single_file_containing_all_ids_to_delete = merge_ids_to_delete_files(path_to_files_containing_ids_to_delete)
    # TODO: implement delete_rows, a function that reads a list of IDs to delete from JSON and launches a mysql query to delete the rows.
    # delete_rows(path_to_single_file_containing_all_ids_to_delete)

    # Set task dependencies
    get_max_id_task >> crawl_table_task
if __name__ == "__main__":
    dag.cli()