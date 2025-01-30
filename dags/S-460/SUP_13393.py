from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from airflow.models.param import Param

# Define constants
#EVENTS_LOG_DIR = '/data/biakonzasftp/S-460/events'
#USAGE_LOG_DIR = '/data/biakonzasftp/S-460/usage/'


EVENTS_LOG_DIR = '/source-biakonzasftp/S-460/events'
USAGE_LOG_DIR = '/source-biakonzasftp/S-460/usage/'
#SESSIONS_LOG_DIR = '/data/biakonzasftp/S-460/sessions/'

# Did not see a connection to prd-az1-opssql.database.windows.net on airflow.konza.org. Please create one before testing
CONN_ID = 'formoperations_prd_az1_opssql_database_windows_net'

# Updated column structures
EVENTS_COLUMNS = [
    'Timestamp', 'Log_Level', 'Source', 'Type', 'Event', 'Username',
    'Client_Address', 'Client_Program', 'Client_Version', 'Client_Desc',
    'Details', 'Session_ID', 'Processing_Timestamp', 'server_source', 'Source_File'
]

USAGE_COLUMNS = [
    'Timestamp', 'Action', 'File_type', 'Username', 'Client_Address', 
    'Client_Desc', 'DiveLine_Filename', 'Local_Filename', 
    'Reason', 'Parent', 'Details', 'Session_ID', 'Processing_Timestamp', 'server_source', 'Source_File'
]


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
}

# Define the DAG
dag = DAG(
    'process_log_files',
    default_args=default_args,
    description='A DAG to process events, usage, and sessions log files and create consolidated DataFrames',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 22),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['S-460'],
    params={
        "max_workers": Param(5, type="integer", minimum=1),
        "batch_size": Param(100, type="integer", minimum=1)
    }
)

# Helper functions
def extract_server_source(file_path, base_dir):
    relative_path = os.path.relpath(file_path, base_dir)
    return relative_path.split(os.sep)[0]  # Extract server folder (e.g., dev-az1-an2)

def format_session_id(session_id):
    session_id = session_id.strip()
    return session_id.replace("-", "").replace(":", "").replace(" ", "_")

def is_file_processed(file_path, table_name, conn_id):
    """Check if a file has already been processed."""
    mssql_hook = MsSqlHook(mssql_conn_id=conn_id)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(1) FROM {table_name} WHERE Source_File = '{file_path}'"
        )
        return cursor.fetchone()[0] > 0
    finally:
        cursor.close()
        connection.close()

def save_data_to_db(data, table_name, conn_id):
    """Save a DataFrame to the database."""
    mssql_hook = MsSqlHook(mssql_conn_id=conn_id)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()
    try:
        for _, row in data.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(f"'{v}'" for v in row)
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
        connection.commit()
    finally:
        cursor.close()
        connection.close()
        
@task(task_id="process_logs_incrementally", dag=dag)
def process_logs_incrementally(base_dir, columns, table_name):
    processed_files = []  # List to track processed files
    skipped_files = []  # List to track files skipped because they were already processed

    for root, subdirs, files in os.walk(base_dir):
        year_in_path = next(
            (part for part in root.split(os.sep) if part.isdigit() and part.startswith("2024") or part.startswith("2025")),
            None
        )
        if not year_in_path:
            continue
                # Log the current directory and subdirectories
        logging.info(f"Processing directory: {root}")
        logging.info(f"Subdirectories found: {subdirs}")
        logging.info(f"Top-level directories: {next(os.walk(base_dir))[1]}")
        
        for file in sorted(files):  # Sort files to process in order
            if file.endswith(".log"):
                file_path = os.path.join(root, file)

                # Check if the file has already been processed
                if is_file_processed(file_path, table_name, CONN_ID):
                    logging.info(f"File {file_path} already processed. Skipping.")
                    skipped_files.append(file_path)  # Add to skipped files list
                    continue

                try:
                    # Process the file
                    df = pd.read_csv(file_path, sep="\t", header=0)
                    raw_file_columns = columns[:-3]
                    if len(df.columns) != len(raw_file_columns):
                        logging.error(
                            f"Column count mismatch in file {file_path}: "
                            f"Expected {len(raw_file_columns)}, Found {len(df.columns)}. Skipping file."
                        )
                        continue

                    df.columns = raw_file_columns
                    df = df.astype(str)
                    df['Processing_Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df['server_source'] = extract_server_source(file_path, base_dir)
                    df['Source_File'] = file_path
                    df['Session_ID'] = df['Session_ID'].apply(format_session_id)

                    save_data_to_db(df, table_name, CONN_ID)
                    processed_files.append(file_path)  # Add to processed files list
                    logging.info(f"File {file_path} processed and saved to {table_name}.")
                except Exception as e:
                    logging.error(f"Error processing file {file_path}: {e}")

    return processed_files, skipped_files  # Return both processed and skipped files

@task(task_id="cleanup_processed_files", dag=dag)
def cleanup_processed_files(file_data):
    """Delete only the files that were processed or skipped due to being already processed, and remove empty folders."""
    processed_files, skipped_files = file_data  # Unpack the processed and skipped files
    files_to_delete = processed_files + skipped_files

    for file_path in files_to_delete:
        try:
            # Delete the file
            os.remove(file_path)
            logging.info(f"Deleted file: {file_path}")

            # Check if the parent folder is now empty
            folder_path = os.path.dirname(file_path)
            if not os.listdir(folder_path):  # If the folder is empty
                os.rmdir(folder_path)  # Delete the empty folder
                logging.info(f"Deleted empty folder: {folder_path}")

        except Exception as e:
            logging.error(f"Error deleting file or folder for {file_path}: {e}")

process_event_logs = process_logs_incrementally.override(task_id="process_event_logs")(
    base_dir=EVENTS_LOG_DIR,
    columns=EVENTS_COLUMNS,
    table_name="dbo.events_logs"
)

process_usage_logs = process_logs_incrementally.override(task_id="process_usage_logs")(
    base_dir=USAGE_LOG_DIR,
    columns=USAGE_COLUMNS,
    table_name="dbo.usage_logs"
)

cleanup_event_logs = cleanup_processed_files.override(task_id="cleanup_event_logs")(
    file_data=process_event_logs
)

cleanup_usage_logs = cleanup_processed_files.override(task_id="cleanup_usage_logs")(
    file_data=process_usage_logs
)
# Define dependencies
process_event_logs >> cleanup_event_logs >> process_usage_logs >> cleanup_usage_logs

if __name__ == "__main__":
    dag.cli()
