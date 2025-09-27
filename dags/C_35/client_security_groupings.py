from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import logging
import pandas as pd
import os

LOCAL_DESTINATION_CSG = '/data/biakonzasftp/C-35/client_security_groupings.csv'
CONTAINER_NAME = "ertacontainer-prd51/clientresults/client_security_groupings"
BLOB_NAME_CSG = "client_security_groupings.csv"

def pull_csg_from_sql(**kwargs):
    # MySQL connection and query
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
    query =  """select
    'Client'
    ,'MPI'
    ,'Provider_NPI'
    ,'Provider_DB_ID'
    ,'Provider_fname'
    ,'Provider_lname'
    ,'server_id'
    ,'visits_found_within_36months'
    ,'event_timestamp'
    UNION
    SELECT * FROM clientresults.client_security_groupings_approved
    union all 
    select * from clientresults.client_security_groupings where client in (select folder_name from clientresults.client_hierarchy where live = 0);"""
   
    
    dfManCat = sql_hook.get_pandas_df(query)
    logging.info(f'Query executed successfully, retrieved {len(dfManCat)} records.')

    # Save DataFrame to CSV locally
    dfManCat.to_csv(LOCAL_DESTINATION_CSG, index=False)
    logging.info(f'Data saved locally to {LOCAL_DESTINATION_CSG}')

def upload_to_azure_blob(**kwargs):
    # Create a WasbHook object
    wasb_hook = WasbHook(wasb_conn_id="hdinsightstorageaccount-blob-core-windows-net")
    
    # Upload the local file to the blob
    wasb_hook.load_file(LOCAL_DESTINATION_CSG, container_name=CONTAINER_NAME, blob_name=BLOB_NAME_CSG, overwrite=True)
    
    logging.info(f'Uploaded {LOCAL_DESTINATION_CSG} to Azure Blob Storage as {BLOB_NAME_CSG}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="pull_csg_sql_data",
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,               
    tags=['c-35']
) as dag:
    
    task_pull_data = PythonOperator(
        task_id="pull_data",
        python_callable=pull_csg_from_sql,
        provide_context=True,
    )

    task_upload_blob = PythonOperator(
        task_id="upload_to_azure_blob",
        python_callable=upload_to_azure_blob,
        provide_context=True,
    )

    task_pull_data >> task_upload_blob
