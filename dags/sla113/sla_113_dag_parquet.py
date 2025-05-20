"""
SLA-113 Parquet Processing DAG

This DAG processes parquet files from Azure Blob Storage by:
1. Reading a CSV file containing accid_ref values to filter out
2. Processing parquet files in specified directories
3. Uploading the filtered results back to Azure Blob Storage

The processing removes rows with accid_ref values that match those in the CSV file.
"""
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import task
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
from typing import List, Tuple
import re
import pandas as pd
from pathlib import Path
import os
import logging
import io

# Configuration constants
AZURE_CONNECTION_NAME = 'reportwriterstorage-blob-core-windows-net'
CONTAINER_NAME = 'content'
SOURCE_PATH = "mpi" #Initial test location on production was "mpi" with prod being "parquet-master-data/mpi"
# Destination prefix for adjusted files which will be appended to the source prefix
DESTINATION_PREFIX = "Adjusted/" #for an extra step of validation set to "Adjusted/" or "" if overwriting directly
DESTINATION_PATH = f"{SOURCE_PATH}/{DESTINATION_PREFIX}"
WORKSHEET_BLOB_PATH = "/source-biakonzasftp/C-9/SLA-113/"
PARALLEL_TASK_LIMIT = 5

def get_azure_connection_string(conn_id: str) -> str:
    """
    Retrieve the Azure Blob Storage connection string from Airflow connections.
    
    Args:
        conn_id: The Airflow connection ID
        
    Returns:
        The Azure connection string
    """
    connection = BaseHook.get_connection(conn_id)
    conn_string = connection.extra_dejson.get("connection_string")
    if not conn_string:
        raise ValueError(f"No connection string found in connection {conn_id}")
    return conn_string

# Get Azure connection string once at DAG definition time
AZURE_CONNECTION_STRING = get_azure_connection_string(AZURE_CONNECTION_NAME)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'SLA-113process_parquet_files',
    default_args=default_args,
    description='Process parquet files from Azure Blob Storage, filtering out rows based on accid_ref values',
    schedule=None,
    tags=['sla113', 'SLA-113', 'parquet', 'azure'],
    concurrency=PARALLEL_TASK_LIMIT,
    catchup=False,
    params={
        "filename": Param("Worksheet_207.csv", type="string", description="Enter a CSV filename to process that has the required single column with the Account ID references (eg. ACCID or ACCID_REF column only)"),
    },
) as dag:
    def get_subdirectories_with_pattern(container_name: str, prefix: str, pattern: str = r"\d{4}-\d{2}") -> List[str]:
        """
        List unique subdirectories in the blob container that match the YYYY-MM pattern.
        
        Args:
        container_name: Name of the Azure Blob container
        prefix: Prefix path to search within
        pattern: Regex pattern to match subdirectory names
        
        Returns:
        List of unique subdirectory names matching the pattern
        """
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(container_name)
           
            subdirs = set()
            blobs = container_client.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                parts = blob.name[len(prefix):].split('/')
                if parts:
                    match = re.match(pattern, parts[0])
                if match:
                    subdirs.add(match.group(0))
        
            return sorted(subdirs)
        except Exception as e:
            logging.error(f"Error listing subdirectories: {e}")
            raise

    def read_csv_from_blob(params) -> pd.DataFrame:
        """
        Read the CSV file with accid_ref values to remove from Azure Blob Storage.

        Returns:
            DataFrame containing accid_ref values to be filtered out

        Raises:
            Exception: If there's an error reading the CSV from blob storage
        """
        filename = params['filename']
        CSVBlobPath = f'{WORKSHEET_BLOB_PATH}/{filename}'
        blob_service_client = None
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
            blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME, 
                blob=CSVBlobPath,
            )

            blob_data = blob_client.download_blob().readall()
            accid_df = pd.read_csv(io.BytesIO(blob_data))
            accid_df.columns = ['accid_ref']
            logging.info(f"Read {len(accid_df)} accid_ref values to filter out")
            return accid_df
        except Exception as e:
            logging.error(f"Error reading CSV from blob: {e}")
            raise

    def process_parquet_file(
        blob_name: str, 
        destination_path: str, 
        accid_df: pd.DataFrame
    ) -> None:
        """
        Process a parquet file from blob storage and upload filtered version.

        Args:
            blob_name: Name of the blob to process
            destination_path: Destination directory prefix
            accid_df: DataFrame containing accid_ref values to filter out

        Raises:
            Exception: If there's an error processing the parquet file
        """
        blob_service_client = None
        try:
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
            blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME, 
                blob=blob_name
            )

            # Download blob data
            blob_data = blob_client.download_blob().readall()

            # Read parquet file
            df = pd.read_parquet(io.BytesIO(blob_data))
            if df.empty:
                logging.info(f'Input blob empty: {blob_name}')
                return

            # Filter out rows
            df_new = df[~df['accid_ref'].isin(accid_df['accid_ref'])]
            rows_removed = len(df.index) - len(df_new.index)
            logging.info(f'{rows_removed} rows removed from: {blob_name}')

            # Prepare destination blob path
            file_name = Path(blob_name).name
            destination_blob_name = os.path.join(destination_path, file_name)

            # Convert back to parquet and upload
            output_buffer = io.BytesIO()
            df_new.to_parquet(output_buffer)
            output_buffer.seek(0)

            # Upload to destination
            destination_blob_client = blob_service_client.get_blob_client(
                container=CONTAINER_NAME, 
                blob=destination_blob_name
            )
            destination_blob_client.upload_blob(output_buffer, overwrite=True)
            logging.info(f'Successfully uploaded processed file to {destination_blob_name}')

        except Exception as e:
            logging.error(f"Error processing parquet file {blob_name}: {e}")
            raise

    @task
    def generate_directory_list_task() -> List[Tuple[str, str]]:
        """
        Generate a list of source and destination directory pairs.
        Excludes directories that only contain files in DESTINATION_PATH folders.

        Returns:
            List of tuples containing (source_directory, destination_directory)
        """
        dirs = []
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        
        SUB_DIRECTORIES = get_subdirectories_with_pattern(CONTAINER_NAME, SOURCE_PATH + "/")
        for suffix in SUB_DIRECTORIES:
            source_sub_directory = os.path.join(SOURCE_PATH, suffix)
            destination_sub_directory = os.path.join(DESTINATION_PATH, suffix)

            # Check if the directory has files that are not in DESTINATION_PATH folders
            blobs = list(container_client.list_blobs(name_starts_with=source_sub_directory))
            non_adjusted_blobs = [blob for blob in blobs if DESTINATION_PREFIX not in blob.name]

            if non_adjusted_blobs:
                logging.info(f"Directory {source_sub_directory} has {len(non_adjusted_blobs)} non-Adjusted files to process")
                dirs.append((source_sub_directory, destination_sub_directory))
            else:
                logging.info(f"Skipping directory {source_sub_directory} as it only contains files in Adjusted folders or is empty")

        logging.info(f"Generated {len(dirs)} directory pairs to process")
        return dirs

    @task
    def process_parquet_files_in_directory(in_out_dirs: Tuple[str, str]) -> None:
        """
        Process all parquet files in a given directory.

        Args:
            in_out_dirs: Tuple containing (source_directory, destination_directory)
        """
        source_path, destination_path = in_out_dirs

        try:
            # Get the list of blobs in the source directory
            blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
            container_client = blob_service_client.get_container_client(CONTAINER_NAME)

            # List blobs with the source prefix and filter out Adjusted folders
            blobs = list(container_client.list_blobs(name_starts_with=source_path))
            filtered_blobs = [blob for blob in blobs if DESTINATION_PREFIX not in blob.name]

            logging.info(f"Processing {len(filtered_blobs)} files in {source_path}")

            if not filtered_blobs:
                logging.info(f"No files to process in {source_path}, skipping")
                return

            # Read the accid_ref CSV once
            accid_df = read_csv_from_blob(params=dag.params)

            # Process each blob
            for blob in filtered_blobs:
                process_parquet_file(blob.name, destination_path, accid_df)

            logging.info(f"Successfully processed all files in {source_path}")

        except Exception as e:
            logging.error(f"Error processing directory {source_path}: {e}")
            raise

    # Generate the list of directories and process each in parallel
    directory_list = generate_directory_list_task()
    processed_files = process_parquet_files_in_directory.expand(in_out_dirs=directory_list)
