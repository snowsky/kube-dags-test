from datetime import datetime
from airflow import DAG
from airflow.decorators import task, dag
from airflow.hooks.base import BaseHook
from airflow.models import Param
from azure.storage.blob import BlobServiceClient
import logging

SOURCE_CONNECTION_ID = 'source_conn_id'
TARGET_CONNECTION_ID = 'target_conn_id'
SOURCE_FILES_DIRECTORY = 'source'
SOURCE_FILES_SUBDIRECTORY = ''
TARGET_FILES_DIRECTORY = 'target'

with DAG(
    dag_id='blob_transfer_hook_uri',
    start_date=datetime(2024, 1, 1),  # Set a fixed start date
    schedule='@daily',
    catchup=False,
    tags=['konza', 'azure_blob_conn', 'uri_string'],
    params={
        "source_connection_id": Param(SOURCE_CONNECTION_ID, type="string"),
        "target_connection_id": Param(TARGET_CONNECTION_ID, type="string"),
        "source_file_directory": Param(SOURCE_FILES_DIRECTORY, type="string"),
        "source_file_subdirectory": Param(SOURCE_FILES_SUBDIRECTORY, type="string"),
        "target_file_directory": Param(TARGET_FILES_DIRECTORY, type="string")
    }
) as dag:
    
    @task
    def transfer_blob_hook_uri():
        """
        Task to transfer blob files from a source folder to a target folder.
        """
        def get_conn_string(connection_id:str):
            try:
                # get connection from hook
                conn = BaseHook.get_connection(connection_id)
                # Retrieve the connection string using get_uri
                uri_string = conn.get_uri()
                conn_string = uri_string.replace("%3D", "=").replace("%3B", ";").replace("%2F", "/").replace("%2B", "+") #working
                logging.info(f"Connection string: {conn_string}")
                return conn_string
            except Exception as e:
                logging.error(f"Error: {str(e)}")
                return None
            
        def transfer_blob(source_conn_str:str, target_conn_str:str, source_dir_name:str, target_dir_name:str, subfolder_dir:str=None):
            # Create the BlobServiceClient object
            source_blob_service_client = BlobServiceClient.from_connection_string(source_conn_str)
            target_blob_service_client = BlobServiceClient.from_connection_string(target_conn_str)
            try:
                # Get the container client
                source_container_client = source_blob_service_client.get_container_client(source_dir_name)
                target_container_client = target_blob_service_client.get_container_client(target_dir_name)
                try:
                    container_info = target_container_client.get_container_properties()
                    logging.info(f"Container named '{container_info.name}' exists.")
                except Exception:
                    logging.info(f"Container '{target_dir_name}' does not exist.")
                    # Recreate the container
                    target_blob_service_client.create_container(target_dir_name)
                    logging.info(f"Container '{target_dir_name}' created.")
                
                # List blobs
                for blob in source_container_client.list_blobs(name_starts_with=subfolder_dir):
                    logging.info(f"Processing blob: {blob.name}")
                    # Get the blob content
                    source_blob_client = source_container_client.get_blob_client(blob.name)
                    blob_data = source_blob_client.download_blob().readall()
                    # Upload the blob to the target container
                    target_blob_client = target_container_client.get_blob_client(blob.name)
                    target_blob_client.upload_blob(blob_data, overwrite=True)
                    logging.info(f"Uploaded blob: {blob.name} to target")
            except Exception as e:
                logging.error(f"Error: {str(e)}")

        conn_source_str = get_conn_string(dag.params['source_connection_id'])
        conn_target_str = get_conn_string(dag.params['target_connection_id'])
        transfer_blob(conn_source_str, 
                      conn_target_str, 
                      dag.params['source_file_directory'], 
                      dag.params['target_file_directory'],
                      dag.params['source_file_subdirectory'])

    transfer_blob_hook_uri()
