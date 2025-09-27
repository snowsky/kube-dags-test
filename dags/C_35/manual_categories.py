from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import logging
import pandas as pd
import os

LOCAL_DESTINATION_MC = '/data/biakonzasftp/C-35/manual_categories.csv'
CONTAINER_NAME = "ertacontainer-prd51/clientresults/manual_categories"
BLOB_NAME_MC = "manual_categories.csv"

def pull_mc_from_sql(**kwargs):
    # MySQL connection and query
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
    query = """select
    'id'
    ,'category'
    ,'measure_group'
    ,'Type'
    ,'cluster_build'
    ,'reporting_period_lookback_date'
    ,'High Risk'
    ,'diagnosis_code'
    ,'procedure_code'
    ,'code_longitudinal_view_months'
    ,'denominator_numerator'
    ,'code_frequency'
    ,'performance_reporting_met'
    ,'metric_reference'
    ,'age_start'
    ,'age_end'
    ,'gender'
    ,'LOINC'
    ,'rxnorm_code'
    ,'medication_name'
    ,'medication_name_like'
    ,'immunization_code'
    ,'immunization_short_name'
    ,'immunization_short_name_like'
    ,'vaccine_short_name'
    ,'vaccine_short_name_like'
    ,'lab_result_value_less_than'
    ,'lab_result_value_greater_than'
    ,'lab_name'
    ,'lab_name_like'
    ,'lab_hl7'
    ,'lab_hl7_like'
    ,'lab_description'
    ,'lab_description_like'
    ,'vitalsigns_name'
    ,'vitalsigns_name_like'
    ,'vitalsigns_description'
    ,'vitalsigns_description_like'
    ,'vitalsigns_code'
    ,'vitalsigns_value_less_than'
    ,'vitalsigns_value_greater_than'
    ,'include_exclude'
    ,'limited_duration_condition'
    ,'event_timestamp' UNION
    SELECT * FROM clientresults.manual_categories;"""
    
    dfManCat = sql_hook.get_pandas_df(query)
    logging.info(f'Query executed successfully, retrieved {len(dfManCat)} records.')

    # Save DataFrame to CSV locally
    dfManCat.to_csv(LOCAL_DESTINATION_MC, index=False)
    logging.info(f'Data saved locally to {LOCAL_DESTINATION_MC}')

def upload_to_azure_blob(**kwargs):
    # Create a WasbHook object
    wasb_hook = WasbHook(wasb_conn_id="hdinsightstorageaccount-blob-core-windows-net")
    
    # Upload the local file to the blob
    wasb_hook.load_file(LOCAL_DESTINATION_MC, container_name=CONTAINER_NAME, blob_name=BLOB_NAME_MC, overwrite=True)
    
    logging.info(f'Uploaded {LOCAL_DESTINATION_MC} to Azure Blob Storage as {BLOB_NAME_MC}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="pull_mc_sql_data",
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False,              
    tags=['c-35']
) as dag:
    
    task_pull_data = PythonOperator(
        task_id="pull_data",
        python_callable=pull_mc_from_sql,
        provide_context=True,
    )

    task_upload_blob = PythonOperator(
        task_id="upload_to_azure_blob",
        python_callable=upload_to_azure_blob,
        provide_context=True,
    )

    task_pull_data >> task_upload_blob
