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
EVENTS_LOG_DIR = '/source-biakonzasftp/S-460/test_logs_aa/events/202412'
USAGE_LOG_DIR = '/source-biakonzasftp/S-460/test_logs_aa/usage/202412'
SESSIONS_LOG_DIR = '/source-biakonzasftp/S-460/test_logs_aa/sessions/202412'
OUTPUT_DIR = '/source-biakonzasftp/S-460/test_logs_aa'

#EVENTS_LOG_DIR = '/data/biakonzasftpS-460/test_logs_aa/events/202412'
#USAGE_LOG_DIR = '/data/biakonzasftpS-460/test_logs_aa/usage/202412'
#SESSIONS_LOG_DIR = '/data/biakonzasftpS-460/test_logs_aa/sessions/202412'
#OUTPUT_DIR = '/data/biakonzasftpS-460/test_logs_aa'

# Did not see a connection to prd-az1-opssql.database.windows.net on airflow.konza.org. Please create one before testing
CONN_ID = 'formoperations_prd_az1_opssql_database_windows_net'

# Updated column structures
EVENTS_COLUMNS = [
    'Timestamp', 'Log_Level', 'Source', 'Type', 'Event', 'Username',
    'Client_Address', 'Client_Program', 'Client_Version', 'Client_Desc',
    'Details', 'Session_ID', 'Processing_Timestamp'
]

USAGE_COLUMNS = [
    'Timestamp', 'Action', 'File_type', 'Username', 'Client_Address', 
    'Client_Desc', 'DiveLine_Filename', 'Local_Filename', 
    'Reason', 'Parent', 'Details', 'Session_ID', 'Processing_Timestamp'
]

SESSIONS_COLUMNS = [
    'Timestamp', 'Event_Type', 'Details', 'Session_ID', 'Processing_Timestamp'
]

#EVENTS_TABLE = 'dbo.events_logs'
#USAGE_TABLE = 'dbo.usage_logs'
#SESSIONS_TABLE = 'dbo.sessions_logs'



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
    start_date=datetime(2025, 1, 14),
    catchup=False,
    tags=['S-460'],
    params={
        "max_workers": Param(5, type="integer", minimum=1),
        "batch_size": Param(100, type="integer", minimum=1)
    }
)


def format_session_id(session_id):
    """
    Simplify Session_ID transformation by treating it as a string.
    Example Converts '2024-11-15 12:01:21 12460'(events and usage) to '20241115_120121_12460'(sessions).
    """
    try:
        session_id = session_id.strip()  # Remove leading/trailing whitespace
        # Replace characters and format as YYYYMMDD_HHMMSS_<ID>
        return session_id.replace("-", "").replace(":", "").replace(" ", "_")
    except Exception as e:
        logging.error(f"Error formatting Session_ID: {repr(session_id)}, Error: {e}")
        return session_id  # Return the original value if transformation fails

        
# Tasks for events
@task(task_id="events_list_log_files", dag=dag)
def events_list_log_files():
    log_files = []
    for root, _, files in os.walk(EVENTS_LOG_DIR):
        log_files.extend(os.path.join(root, file) for file in files if file.endswith(".log"))
    return log_files

@task(task_id="events_process_log_files", dag=dag)
def process_event_logs(log_files):
    """Process event log files into a consolidated DataFrame."""
    all_data = []
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for file_path in log_files:
        try:
            df = pd.read_csv(file_path, sep="\t", header=0)
            df.columns = EVENTS_COLUMNS[:-1]  # Exclude 'Processing_Timestamp'
            df = df.astype(str)
            df['Processing_Timestamp'] = current_timestamp
            df['Session_ID'] = df['Session_ID'].apply(format_session_id)
            all_data.append(df)
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
    
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=EVENTS_COLUMNS)

@task(task_id="save_events_to_csv_and_db", dag=dag)
def save_events_to_csv_and_db(consolidated_df):
    """
    Save the consolidated DataFrame to a CSV file and append it to the database table dbo.events_logs.
    Checks the Processing_Timestamp column to ensure only new records are added.
    """
    # Save the DataFrame to a CSV file
    datetime_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(OUTPUT_DIR, f"consolidated_event_logs_{datetime_suffix}.csv")
    consolidated_df.to_csv(output_file, index=False)
    logging.info(f"Consolidated log file saved to: {output_file}")

    # Connect to the database and fetch the latest Processing_Timestamp
    mssql_hook = MsSqlHook(mssql_conn_id=CONN_ID)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()

    try:
        # Fetch the latest Processing_Timestamp from the table
        cursor.execute(f"SELECT MAX(Processing_Timestamp) FROM dbo.events_logs")
        max_processing_timestamp = cursor.fetchone()[0]
        if max_processing_timestamp:
            logging.info(f"Latest Processing_Timestamp in the table: {max_processing_timestamp}")
        else:
            logging.info("Table is empty. All records will be inserted.")

        # Filter new records based on Processing_Timestamp
        if max_processing_timestamp:
            new_data = consolidated_df[
                consolidated_df['Processing_Timestamp'] > max_processing_timestamp
            ]
        else:
            new_data = consolidated_df

        # Check if there is any new data to insert
        if new_data.empty:
            logging.info("No new data to append to the dbo.events_logs table.")
            return

        # Insert new records into the database
        for _, row in new_data.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(f"'{v}'" for v in row)
            insert_query = f"INSERT INTO dbo.events_logs ({columns}) VALUES ({values})"
            cursor.execute(insert_query)

        connection.commit()
        logging.info(f"Appended {len(new_data)} new records to the dbo.events_logs table.")

    except Exception as e:
        connection.rollback()
        logging.error(f"Error appending data to the dbo.events_logs table: {e}")

    finally:
        cursor.close()
        connection.close()


# Tasks for usage
@task(task_id="usage_list_log_files", dag=dag)
def usage_list_log_files():
    log_files = []
    for root, _, files in os.walk(USAGE_LOG_DIR):
        log_files.extend(os.path.join(root, file) for file in files if file.endswith(".log"))
    return log_files

@task(task_id="usage_process_log_files", dag=dag)
def process_usage_logs(log_files):
    """Process usage log files into a consolidated DataFrame."""
    all_data = []
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for file_path in log_files:
        try:
            df = pd.read_csv(file_path, sep="\t", header=0)
            df.columns = USAGE_COLUMNS[:-1]  # Exclude 'Processing_Timestamp'

            # Assign all columns as strings (nvarchar-like behavior)
            df = df.astype(str)
            df['Processing_Timestamp'] = current_timestamp

            # Transform Session_ID
            df['Session_ID'] = df['Session_ID'].apply(format_session_id)
            all_data.append(df)
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
    
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=USAGE_COLUMNS)

@task(task_id="save_usage_to_csv_and_db", dag=dag)
def save_usage_to_csv_and_db(consolidated_df):
    """
    Save the consolidated DataFrame to a CSV file and append it to the database table dbo.events_logs.
    Checks the Processing_Timestamp column to ensure only new records are added.
    """
    # Save the DataFrame to a CSV file
    datetime_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(OUTPUT_DIR, f"consolidated_usage_logs_{datetime_suffix}.csv")
    consolidated_df.to_csv(output_file, index=False)
    logging.info(f"Consolidated log file saved to: {output_file}")

    # Connect to the database and fetch the latest Processing_Timestamp
    mssql_hook = MsSqlHook(mssql_conn_id=CONN_ID)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()

    try:
        # Fetch the latest Processing_Timestamp from the table
        cursor.execute(f"SELECT MAX(Processing_Timestamp) FROM dbo.usage_logs")
        max_processing_timestamp = cursor.fetchone()[0]
        if max_processing_timestamp:
            logging.info(f"Latest Processing_Timestamp in the table: {max_processing_timestamp}")
        else:
            logging.info("Table is empty. All records will be inserted.")

        # Filter new records based on Processing_Timestamp
        if max_processing_timestamp:
            new_data = consolidated_df[
                consolidated_df['Processing_Timestamp'] > max_processing_timestamp
            ]
        else:
            new_data = consolidated_df

        # Check if there is any new data to insert
        if new_data.empty:
            logging.info("No new data to append to the dbo.usage_logs table.")
            return

        # Insert new records into the database
        for _, row in new_data.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(f"'{v}'" for v in row)
            insert_query = f"INSERT INTO dbo.usage_logs ({columns}) VALUES ({values})"
            cursor.execute(insert_query)

        connection.commit()
        logging.info(f"Appended {len(new_data)} new records to the dbo.usage_logs table.")

    except Exception as e:
        connection.rollback()
        logging.error(f"Error appending data to the dbo.usage_logs table: {e}")

    finally:
        cursor.close()
        connection.close()


# Tasks for sessions
@task(task_id="sessions_list_log_files", dag=dag)
def sessions_list_log_files():
    log_files = []
    for root, _, files in os.walk(SESSIONS_LOG_DIR):
        log_files.extend(os.path.join(root, file) for file in files if file.endswith(".log"))
    return log_files

@task(task_id="sessions_process_log_files", dag=dag)
def process_session_logs(log_files):
    """Process session log files into a consolidated DataFrame."""
    all_data = []
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for file_path in log_files:
        try:
            session_id = os.path.basename(file_path).replace('.log', '')
            df = pd.read_csv(file_path, sep="\t", header=0, names=SESSIONS_COLUMNS[:-2])

            # Assign all columns as strings (nvarchar-like behavior)
            df = df.astype(str)
            df['Processing_Timestamp'] = current_timestamp
            df['Session_ID'] = session_id
            all_data.append(df)
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
    
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=SESSIONS_COLUMNS)


@task(task_id="save_sessions_to_csv_and_db", dag=dag)
def save_sessions_to_csv_and_db(consolidated_df):
    """
    Save the consolidated DataFrame to a CSV file and append it to the database table dbo.sessions_logs.
    Checks the Processing_Timestamp column to ensure only new records are added.
    """
    # Save the DataFrame to a CSV file
    datetime_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(OUTPUT_DIR, f"consolidated_sessions_logs_{datetime_suffix}.csv")
    consolidated_df.to_csv(output_file, index=False)
    logging.info(f"Consolidated log file saved to: {output_file}")

    # Connect to the database and fetch the latest Processing_Timestamp
    mssql_hook  = MsSqlHook(mssql_conn_id=CONN_ID)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()

    try:
        # Fetch the latest Processing_Timestamp from the table
        cursor.execute(f"SELECT MAX(Processing_Timestamp) FROM dbo.sessions_logs")
        max_processing_timestamp = cursor.fetchone()[0]
        if max_processing_timestamp:
            logging.info(f"Latest Processing_Timestamp in the table: {max_processing_timestamp}")
        else:
            logging.info("Table is empty. All records will be inserted.")

        # Filter new records based on Processing_Timestamp
        if max_processing_timestamp:
            new_data = consolidated_df[
                consolidated_df['Processing_Timestamp'] > max_processing_timestamp
            ]
        else:
            new_data = consolidated_df

        # Check if there is any new data to insert
        if new_data.empty:
            logging.info("No new data to append to the dbo.sessions_logs table.")
            return

        # Insert new records into the database
        for _, row in new_data.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(f"'{v}'" for v in row)
            insert_query = f"INSERT INTO dbo.sessions_logs ({columns}) VALUES ({values})"
            cursor.execute(insert_query)

        connection.commit()
        logging.info(f"Appended {len(new_data)} new records to the dbo.sessions_logs table.")

    except Exception as e:
        connection.rollback()
        logging.error(f"Error appending data to the dbo.sessions_logs table: {e}")

    finally:
        cursor.close()
        connection.close()


event_log_files = events_list_log_files()
processed_event_logs = process_event_logs(event_log_files)
save_events_to_csv_and_db_task = save_events_to_csv_and_db(processed_event_logs)

usage_log_files = usage_list_log_files()
processed_usage_logs = process_usage_logs(usage_log_files)
save_usage_to_csv_and_db_task = save_usage_to_csv_and_db(processed_usage_logs)

session_log_files = sessions_list_log_files()
processed_session_logs = process_session_logs(session_log_files)  # Task defined correctly
save_sessions_to_csv_and_db_task = save_sessions_to_csv_and_db(processed_session_logs)  # Use the correct variable


# Define dependencies
event_log_files >> processed_event_logs >> save_events_to_csv_and_db_task >> usage_log_files >> processed_usage_logs >> save_usage_to_csv_and_db_task >> session_log_files >> processed_session_logs >> save_sessions_to_csv_and_db_task
#create_usage_table_task >> usage_log_files >> processed_usage_logs >> latest_usage_ts >> save_usage_to_db
#create_sessions_table_task >> session_log_files >> processed_session_logs >> latest_session_ts >> save_sessions_to_db

if __name__ == "__main__":
    dag.cli()