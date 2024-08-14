from airflow import DAG
from airflow.decorators import task
from operators.mysql.mysql_to_file_operator import MySqlToFileOperator
from operators.mysql.mysql_to_blob_storage_operator import MySqlToBlobStorageOperator


from test.constants import (
  SYNTHETIC_MPI_TABLE_SCHEMA,
  SYNTHETIC_MPI_TABLE_NAME,
  SYNTHETIC_MPI_CONNECTION_ID,
  TEST_OPTOUT_FILES_DIRECTORY,
)

EXAMPLE_OUTPUT_PATH = '/tmp/this-should-be-a-durable-path-in-a-real-dag.csv'
EXAMPLE_AZURE_BLOB_STORAGE_CONNECTION_ID = 'azure-blob-storage-test'

with DAG(
    'example_mysql_to_file',
    default_args={'owner': 'airflow'},
    schedule=None,
    tags=['example', 'data-manipulation'],
) as dag:

    data_written_to_file = MySqlToFileOperator(
        task_id="write_data_to_file",
        sql=f"""
        SELECT * FROM {SYNTHETIC_MPI_TABLE_SCHEMA}.{SYNTHETIC_MPI_TABLE_NAME} 
        """,
        mysql_conn_id=SYNTHETIC_MPI_CONNECTION_ID,
        output_file_path=EXAMPLE_OUTPUT_PATH,
    )
    
    data_written_to_blob_storage = MySqlToBlobStorageOperator(
        task_id="write_data_to_blob_storage",
        mysql_conn_id=SYNTHETIC_MPI_CONNECTION_ID,
        azure_blob_storage_conn_id=EXAMPLE_AZURE_BLOB_STORAGE_CONNECTION_ID,
        sql=f"""
        SELECT * FROM {SYNTHETIC_MPI_TABLE_SCHEMA}.{SYNTHETIC_MPI_TABLE_NAME} 
        """,
        output_path=EXAMPLE_OUTPUT_PATH,
        output_container='test',        
    )
