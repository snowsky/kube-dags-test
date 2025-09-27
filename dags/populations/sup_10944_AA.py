import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
import os
import csv
import logging
from populations.target_population_impl import _fix_engine_if_invalid_params
from datetime import datetime
from populations.formoperations_connection import CONNECTION_NAME, EXAMPLE_DATA_PATH

# Define the tasks as functions
def list_csv_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.csv')]

def extract_date_from_filename(filename):
    date_str = filename.split()[2][:8]
    return datetime.strptime(date_str, '%Y%m%d')

def sort_files_by_date(files):
    return sorted(files, key=extract_date_from_filename)

def select_recent_files(files_sorted, directory):
    if len(files_sorted) >= 2:
        latest_file = os.path.join(directory, files_sorted[-1])
        previous_file = os.path.join(directory, files_sorted[-2])
        return latest_file, previous_file
    else:
        raise ValueError("Not enough files to compare. At least two files are required.")

def read_and_clean_csv(file_path, columns_to_keep):
    rows = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)
        
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
            data[column] = pd.NA
    return data[columns_to_keep]

def add_columns_and_edit(raw_file_prev, raw_file_new):
    raw_file_new['Last Seen Date'] = ''
    new_hcids = set(raw_file_new['HCID']) - set(raw_file_prev['HCID'])
    today_date = datetime.today().strftime('%Y-%m-%d')
    raw_file_new.loc[raw_file_new['HCID'].isin(new_hcids), 'First Seen Date'] = today_date

    raw_file_first_edit = raw_file_new.copy()
    raw_file_first_edit['Part of HCID'] = raw_file_first_edit['Part of HCID'].str.split('/').str.get(1)
    raw_file_first_edit['QHIN HCID'] = raw_file_first_edit['QHIN HCID'].str.split('/').str.get(1)

    raw_file_second_edit = raw_file_first_edit.copy()
    raw_file_second_edit['Part Of Name'] = ''
    raw_file_second_edit['QHIN Name'] = ''
    raw_file_second_edit.rename(columns={'Part of': 'Part of HCID', 'QHIN': 'QHIN HCID'}, inplace=True)
    hcid_to_name = pd.Series(raw_file_new['Name'].values, index=raw_file_new['HCID']).to_dict()
    raw_file_second_edit['Part Of Name'] = raw_file_second_edit['Part of HCID'].map(hcid_to_name).fillna('NA')
    raw_file_second_edit['QHIN Name'] = raw_file_second_edit['QHIN HCID'].map(hcid_to_name).fillna('NA')
    logging.info(print(raw_file_second_edit))
    hook = MsSqlHook(mssql_conn_id='formoperations_prd_az1_opssql_database_windows_net')
    engine = hook.get_sqlalchemy_engine()
    raw_file_second_edit.to_sql(name=raw_file_second_edit, con=engine, if_exists="replace")

    #assert False
    #return raw_file_second_edit 




def add_dataframe_to_fromoperations(raw_file_second_edit):
    hook = MsSqlHook(mssql_conn_id='formoperations_prd_az1_opssql_database_windows_net')
    engine = hook.get_sqlalchemy_engine()

    conn = hook.get_conn()
    table_name = 'raw_file_second_edit'
    full_table_name = f"dbo.{table_name}"
    logging.info(print(full_table_name)) 

     
    table_records.to_sql(name=db_table_name, con=engine, if_exists="replace")


    
    data_to_insert = raw_file_second_edit.values.tolist()
    #data_to_insert_row = data_to_insert[0]
    
    #logging.info(print(raw_file_second_edit.head())
    logging.info(print(data_to_insert))

    #return data_to_insert
    cursor = conn.cursor()
    cursor.executemany(insert_stmt, data_to_insert)
    #cursor.executemany(insert_stmt, data_to_insert_row)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info(print(data_to_insert))
    #assert False

 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the directory and columns to keep
directory = '/data/biakonzasftp/C-113/'

columns_to_keep = ['Name', 'HCID', 'Street', 'City', 'State', 'Zip', 'Country', 
                   'Part of HCID', 'Sequoia Org Type', 'Active', 'QHIN HCID', 
                   'LastUpdated', 'POU', 'Unnamed:13', 'Unnamed:14', 'Unnamed:15', 
                   'Unnamed:16', 'Unnamed:17']

# Define the DAG
with DAG(
    'C_113_Information_Request_SUP_10944',
    default_args=default_args,
    description='Pipeline for the ingestion of data related to SUP-9850 : QHIN reporting – RCE Directory and QHIN transactions.Two manual reporting efforts have outgrown an excel based approach and need to be moved to a DB structure',
    schedule_interval=None, 
    start_date=days_ago(2),
    tags=['AA_DAG'],
) as dag:
    @task 
    def read_process_upload_csv_files(directory, db_table_name):
        files_list = [f for f in os.listdir(directory) if f.endswith('.csv')]
        sorted_file_list = sorted(files_list, key=extract_date_from_filename) 
        if len(sorted_file_list) < 2:
            return None
        penultimate_file_path = os.path.join(directory,sorted_file_list[-2])
        last_file_path = os.path.join(directory,sorted_file_list[-1])
        
        def read_and_clean_csv(file_path, columns_to_keep):
            rows = []
            with open(file_path, 'r') as csvfile:
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                
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
                    data[column] = pd.NA
            return data[columns_to_keep]

            # Read and clean the previous and latest CSV files
        raw_file_prev = read_and_clean_csv(penultimate_file_path, columns_to_keep)
        raw_file_new = read_and_clean_csv(last_file_path, columns_to_keep)
        
        # Add column Last Seen Date in raw_file_new
        
        raw_file_new['Last Seen Date'] = ''
        new_hcids = set(raw_file_new['HCID']) - set(raw_file_prev['HCID'])
        today_date = datetime.today().strftime('%Y-%m-%d')
        raw_file_new.loc[raw_file_new['HCID'].isin(new_hcids), 'First Seen Date'] = today_date
        
        # First edit : Done by Robert 
        raw_file_first_edit = raw_file_new.copy()
        raw_file_first_edit['Part of HCID'] = raw_file_first_edit['Part of HCID'].str.split('/').str.get(1)
        raw_file_first_edit['QHIN HCID'] = raw_file_first_edit['QHIN HCID'].str.split('/').str.get(1)
        
        # Second Edit : Done by Thomas
        raw_file_second_edit = raw_file_first_edit.copy()
        raw_file_second_edit['Part Of Name'] = ''
        raw_file_second_edit['QHIN Name'] = ''
        raw_file_second_edit.rename(columns={'Part of': 'Part of HCID', 'QHIN': 'QHIN HCID'}, inplace=True)
        hcid_to_name = pd.Series(raw_file_new['Name'].values, index=raw_file_new['HCID']).to_dict()
        raw_file_second_edit['Part Of Name'] = raw_file_second_edit['Part of HCID'].map(hcid_to_name).fillna('NA')
        raw_file_second_edit['QHIN Name'] = raw_file_second_edit['QHIN HCID'].map(hcid_to_name).fillna('NA')
        
        #Send the raw_file_second_edit to formoperations 
        
        #raw_file_second_edit.columns
        hook = MsSqlHook(mssql_conn_id='formoperations_prd_az1_opssql_database_windows_net')
        #engine = hook.get_sqlalchemy_engine() 
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())

        raw_file_second_edit.to_sql(name=db_table_name, con=engine, if_exists="replace")

    read_process_upload_csv_files_task = read_process_upload_csv_files(
        directory='/data/biakonzasftp/C-113/', 
        db_table_name='c_113_rce_file_export',
    ) 
    



