from datetime import datetime
from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from azure.storage.blob import BlobServiceClient
import logging

@dag(
    dag_id='blob_transfer_hook_uri',
    start_date=days_ago(1),  # Set a fixed start date
    schedule_interval='@daily',
    catchup=False,
    tags=['hoang', 'azure_blob_conn', 'uri_string'],
)

def transfer_blob_hook_uri():
    """
    DAG to transfer blob files from a source folder to a destination folder.
    """
    @task()
    def get_conn_string(connection_id:str):
        try:
            # get connection from hook
            conn = BaseHook.get_connection(connection_id)
            # Retrieve the connection string using get_uri
            uri_string = conn.get_uri()
            conn_string = uri_string.replace("%3D", "=").replace("%3B", ";").replace("%2F", "/").replace("%2B", "+") #working
            logging.error(f"Connection string: {conn_string}")
            return conn_string
        except Exception as e:
            logging.error(f"Error: {str(e)}")
           
    @task()
    def transfer_blob(source_conn_str:str, destination_conn_str:str, source_dir_name:str, destination_dir_name:str, subfolder_dir:str=None):
        # Create the BlobServiceClient object
        source_blob_service_client = BlobServiceClient.from_connection_string(source_conn_str)
        destination_blob_service_client = BlobServiceClient.from_connection_string(destination_conn_str)
        try:
            # Get the container client
            source_container_client = source_blob_service_client.get_container_client(source_dir_name)
            destination_container_client = destination_blob_service_client.get_container_client(destination_dir_name)
            try:
                container_info = destination_container_client.get_container_properties()
                logging.error(f"Container named '{container_info.name}' exists.")
            except Exception:
                logging.error(f"Container '{destination_dir_name}' does not exist.")
                # Recreate the container
                destination_blob_service_client.create_container(destination_dir_name)
                logging.error(f"Container '{destination_dir_name}' created.")
               
            # List blobs
            for blob in source_container_client.list_blobs(name_starts_with=subfolder_dir):
                logging.error(f"Processing blob: {blob.name}")
                # Get the blob content
                source_blob_client = source_container_client.get_blob_client(blob.name)
                blob_data = source_blob_client.download_blob().readall()
                # Upload the blob to the destination container
                destination_blob_client = destination_container_client.get_blob_client(blob.name)
                destination_blob_client.upload_blob(blob_data, overwrite=True)
                logging.error(f"Uploaded blob: {blob.name} to destination")
        except Exception as e:
            logging.error(f"Error: {str(e)}")

    conn_source_str = get_conn_string("source_conn_id")
    conn_destination_str = get_conn_string("destination_conn_id")
    transfer_blob(conn_source_str, conn_destination_str, "source", "destination")

transfer_blob_hook_uri()