import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
import os
import csv
import logging
import re
from datetime import datetime

# Function to extract date from filename
def extract_date_from_filename(filename):
    try:
        date_pattern = re.search(r'(\d{8,14})', filename)
        if date_pattern:
            date_str = date_pattern.group(1)[:8]
            extracted_date = datetime.strptime(date_str, '%Y%m%d')
            logging.info(f"Extracted date {extracted_date} from filename: {filename}")
            
            # Ensure the file date is not before 2025-01-01
            if extracted_date < datetime.strptime('2025-01-01', '%Y-%m-%d'):
                logging.warning(f"Skipping file {filename} - file_date {extracted_date} is before 2025-01-01.")
                return None  # This will ensure the file is ignored
            
            return extracted_date
        else:
            logging.error(f"No valid date found in filename: {filename}. Failing DAG.")
            raise AirflowFailException(f"Failed to extract valid date from filename: {filename}")
    except ValueError as e:
        logging.error(f"Failed to extract date from filename: {filename} - Error: {e}. Failing DAG.")
        raise AirflowFailException(f"Invalid date format in filename: {filename}")

# Reads CSV and cleans data
def read_and_clean_csv(file_path, columns_to_keep):
    rows = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)
        header = [col if col else f'_unnamed_{i}' for i, col in enumerate(header)]
        header = [col.replace('>', '').strip() for col in header]
        for row in csvreader:
            if len(row) != len(header):
                while len(row) < len(header):
                    row.append('')
                while len(row) > len(header):
                    row.pop()
            rows.append(row)
    data = pd.DataFrame(rows, columns=header)
    for column in columns_to_keep:
        if column not in data.columns:
            data[column] = 'Not Applicable'
    return data[columns_to_keep]

# Inserts DataFrame into SQL
def insert_dataframe_to_sql(df, table_name, conn_id):
    mssql_hook = MsSqlHook(mssql_conn_id=conn_id)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()
    df.columns = df.columns.astype(str).fillna("UnknownColumn").str.strip()

    #  Ensure `file_date` never contains `'Not Applicable'`
    if 'file_date' in df.columns:
        df['file_date'] = pd.to_datetime(df['file_date'], errors='coerce')
        df = df.dropna(subset=['file_date'])  # Drop rows where file_date is NaN
        df['file_date'] = df['file_date'].dt.strftime('%Y-%m-%d')

    logging.info(f"DataFrame before SQL insertion:\n{df.head()}")

    columns = ", ".join([f"[{col}]" for col in df.columns])
    data = list(df.itertuples(index=False, name=None))
    for idx, row in enumerate(data):
        try:
            values = "', '".join([str(value).replace("'", "''") for value in row])
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ('{values}')"
            cursor.execute(insert_query)
            connection.commit()
        except Exception as e:
            connection.rollback()
            logging.error(f"Error inserting row {idx + 1}: {e}")
            logging.error(f"Failed Row Data: {row}")
    cursor.close()
    connection.close()

# Get the latest valid file_date from the database
def get_latest_file_date(conn_id, table_name):
    mssql_hook = MsSqlHook(mssql_conn_id=conn_id)
    connection = mssql_hook.get_conn()
    cursor = connection.cursor()
    starting_date = datetime.strptime('2025-02-01', '%Y-%m-%d')  # 
    latest_date = starting_date
    try:
        cursor.execute(f"SELECT MAX(file_date) FROM {table_name} WHERE file_date IS NOT NULL AND file_date != 'Not Applicable'")
        result = cursor.fetchone()
        if result[0]:
            latest_date = datetime.strptime(result[0], '%Y-%m-%d')
        logging.info(f"Latest processed file_date from SQL: {latest_date}")
    except Exception as e:
        logging.error(f"Error fetching latest valid file date: {e}")
    finally:
        cursor.close()
        connection.close()
    return latest_date
# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2025, 2, 27),  
}

CONN_ID = 'formoperations_prd_az1_opssql_database_windows_net'
#directory = '/data/biakonzasftp/C-113/'
directory = '/source-biakonzasftp/C-113/'

columns_to_keep = ['Name', 'HCID', 'Street', 'City', 'State', 'Zip', 'Country', 'Part of HCID', 'Sequoia Org Type', 'Active', 'QHIN HCID', 'LastUpdated', 'POU', 'FirstSeenDate', 'PartOfName', 'QHIN_Name', 'file_date']

# DAG Definition
with DAG(
    'C_113_Information_Request_SUP_10944',
    default_args=default_args,
    description='Pipeline for ingestion of RCE reporting data',
    schedule_interval='@daily', 
    tags=['C-113', 'Information Request'],
) as dag:

    @task
    def process_files(directory, db_table_name):
        latest_processed_date = get_latest_file_date(CONN_ID, db_table_name) or datetime.strptime('2014-12-09', '%Y-%m-%d')
        logging.info(f"Processing files newer than: {latest_processed_date}")

        # List available files
        all_files = os.listdir(directory)
        logging.info(f"Available files in directory: {all_files}")

        # Filter CSV files
        files_list = [f for f in all_files if f.endswith('.csv')]
        logging.info(f"Filtered CSV files: {files_list}")

        # Extract and log dates
        files_with_dates = [(f, extract_date_from_filename(f)) for f in files_list]
        for f, extracted_date in files_with_dates:
            logging.info(f"File: {f} -> Extracted Date: {extracted_date}")

        # Filter files newer than latest processed date
        files_list = [f for f, extracted_date in files_with_dates if extracted_date and extracted_date > latest_processed_date]
        sorted_files = sorted(files_list, key=extract_date_from_filename)
        logging.info(f"Files to be processed (sorted): {sorted_files}")

        if not sorted_files:
            logging.info("No new files to process.")
            return

        for file_name in sorted_files:
            file_path = os.path.join(directory, file_name)
            file_date = extract_date_from_filename(file_name)

            if not file_date:
                logging.error(f"Skipping file due to invalid date format: {file_name}")
                continue

            logging.info(f"Processing file: {file_name} with file_date: {file_date}")

            file_date = file_date.date()
            raw_file_new = read_and_clean_csv(file_path, columns_to_keep)

            mssql_hook = MsSqlHook(mssql_conn_id=CONN_ID)
            connection = mssql_hook.get_conn()
            raw_file_prev = pd.read_sql(f"SELECT * FROM {db_table_name}", connection)
            connection.close()

            new_hcids = set(raw_file_new['HCID']) - set(raw_file_prev['HCID'])
            raw_file_new.loc[raw_file_new['HCID'].isin(new_hcids), 'FirstSeenDate'] = str(file_date)

            raw_file_second_edit = raw_file_new.copy()
            raw_file_second_edit['Part of HCID'] = raw_file_second_edit['Part of HCID'].apply(lambda x: x.split('/')[1] if '/' in x else x)
            raw_file_second_edit['QHIN HCID'] = raw_file_second_edit['QHIN HCID']

            hcid_to_name = pd.Series(raw_file_new['Name'].values, index=raw_file_new['HCID']).to_dict() 
            logging.info (hcid_to_name)
            raw_file_second_edit['PartOfName'] = raw_file_second_edit['Part of HCID'].map(hcid_to_name).fillna('NA')
            raw_file_second_edit['QHIN_Name'] = raw_file_second_edit['QHIN HCID'].map(hcid_to_name).fillna('NA')
            raw_file_second_edit['file_date'] = file_date.strftime('%Y-%m-%d')

            insert_dataframe_to_sql(raw_file_second_edit, db_table_name, CONN_ID)
            logging.info(f"Inserted data from {file_name} into SQL Server.")

    process_files_task = process_files(directory=directory, db_table_name='c_113_rce_file')
