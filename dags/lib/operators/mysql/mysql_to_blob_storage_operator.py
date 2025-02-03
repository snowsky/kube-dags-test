from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
from azure.storage.blob import BlobServiceClient

import logging
import os
import tempfile


class MySqlToBlobStorageOperator(MySqlOperator):
    """
    Executes sql code in a specific MySQL database and downloads the results to a file which is then copied to
    Azure Blob Storage.

    Arguments:
        
        task_id (str): The airflow task id.
        mysql_conn_id (str): The name of the MySQL connection ID.
        azure_blob_storage_conn_id (str): The name of the Azure Blob Storage Connection ID.
        sql (str): the sql to be executed against the database. Rows returned by this SQL statement
            will be written to a file.
        output_container (str): the container in which the results will be written.
        output_path (str): the path at which the results of running the SQL statement will be written. 
        *args: other positional args to be passed to [MySqlOperator](https://airflow.apache.org/docs/apache-airflow-providers-mysql/stable/_api/airflow/providers/mysql/operators/mysql/index.html#airflow.providers.mysql.operators.mysql.MySqlOperator)
        **kwargs: other positional args to be passed to MySqlOperator

    See `dags/examples/example_mysql_to_file.py` for an example DAG using this operator. 

    Note that:
        - the file will be overwritten every time the operator runs.
        - the operator is only tested when using a connection string for the Azure Blob Storage connection. 
          In particular, SAS-based connections may not work.
    """   

 
    def __init__(
        self, 
        task_id: str,
        mysql_conn_id: str,
        azure_blob_storage_conn_id: str,
        sql: str,
        output_container: str,
        output_path: str, 
        *args: str,
        **kwargs: str,
    ):
        super(MySqlToBlobStorageOperator, self).__init__(
            sql=sql, mysql_conn_id=mysql_conn_id, task_id=task_id,
            *args, **kwargs
        )
        self.output_container = output_container
        self.output_path = output_path
        self.azure_blob_storage_conn_id = azure_blob_storage_conn_id

    def execute(self, context):
        hook = MySqlHook(mysql_conn_id=self.conn_id)
        dt = hook.get_pandas_df(self.sql)
        
        conn = BaseHook.get_connection(self.azure_blob_storage_conn_id)
        with tempfile.TemporaryDirectory() as td:
            output_file_path = os.path.join(td, 'data.csv')
            dt.to_csv(output_file_path)

            # get connection from hook
            # Retrieve the connection string using get_uri
            uri_string = conn.get_uri()
            
            sanitized_uri = (
                uri_string
                .replace("%3D", "=")
                .replace("%3B", ";")
                .replace("%2F", "/")
                .replace("%2B", "+")
            )
            destination_blob_service_client = BlobServiceClient.from_connection_string(sanitized_uri)
            destination_container_client = destination_blob_service_client.get_container_client(self.output_container)
            destination_blob_client = destination_container_client.get_blob_client(self.output_path)
            with open(output_file_path, 'rb') as blob_data:
                logging.info(f"Writing {len(dt)} rows to {self.output_path} in the {self.output_container} container.")
                destination_blob_client.upload_blob(blob_data, overwrite=True)
