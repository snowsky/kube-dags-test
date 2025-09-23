from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging
import pandas as pd
import os
import trino
from lib.operators.konza_trino_operator import KonzaTrinoOperator
from trino.dbapi import connect

LOCAL_DESTINATION_MC = '/data/biakonzasftp/C-35/manual_categories_trino.parquet'
LOCAL_DESTINATION_CSG = '/data/biakonzasftp/C-35/client_security_groupings_trino.parquet'
CONTAINER_NAME_MC = "content/parquet-master-data/manual_categories_trino"
BLOB_NAME_MC = "manual_categories_trino"
CONTAINER_NAME_CSG = "content/parquet-master-data/client_security_groupings_trino"
BLOB_NAME_CSG = "client_security_groupings_trino"

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
    SELECT * FROM clientresults.manual_categories
    where category not in (select metric from clientresults.data_refresh_metric_sunset_lookup_list);"""
    
    dfManCat = sql_hook.get_pandas_df(query)
    logging.info(f'Query executed successfully, retrieved {len(dfManCat)} records.')

    # Save DataFrame to Parquet locally
    dfManCat.to_parquet(LOCAL_DESTINATION_MC, index=False)
    logging.info(f'Data saved locally to {LOCAL_DESTINATION_MC}')

drop_mc_table = KonzaTrinoOperator(
    task_id='drop_manual_categories',
    query="DROP TABLE IF EXISTS hive.parquet_master_data.manual_categories_trino"
)
create_mc_table = KonzaTrinoOperator(
    task_id='create_manual_categories',
    query="""
        CREATE TABLE hive.parquet_master_data.manual_categories_trino (
            id varchar,
            category varchar,
            measure_group varchar,
            Type varchar,
            cluster_build varchar,
            reporting_period_lookback_date varchar,
            "High Risk" varchar, 
            diagnosis_code varchar,
            procedure_code varchar,
            code_longitudinal_view_months varchar,
            denominator_numerator varchar,
            code_frequency varchar,
            performance_reporting_met varchar,
            metric_reference varchar,
            age_start varchar,
            age_end varchar,
            gender varchar,
            LOINC varchar,
            rxnorm_code varchar,
            medication_name varchar,
            medication_name_like varchar,
            immunization_code varchar,
            immunization_short_name varchar,
            immunization_short_name_like varchar,
            vaccine_short_name varchar,
            vaccine_short_name_like varchar,
            lab_result_value_less_than varchar,
            lab_result_value_greater_than varchar,
            lab_name varchar,
            lab_name_like varchar,
            lab_hl7 varchar,
            lab_hl7_like varchar,
            lab_description varchar,
            lab_description_like varchar,
            vitalsigns_name varchar,
            vitalsigns_name_like varchar,
            vitalsigns_description varchar,
            vitalsigns_description_like varchar,
            vitalsigns_code varchar,
            vitalsigns_value_less_than varchar,
            vitalsigns_value_greater_than varchar,
            include_exclude varchar,
            limited_duration_condition varchar,
            event_timestamp varchar
        )
        WITH (
    external_location = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/manual_categories_trino',
    format = 'PARQUET'
)
    """
)

def upload_to_azure_blob(**kwargs):
    # Create a WasbHook object
    wasb_hook = WasbHook(wasb_conn_id="reportwriterstorage-blob-core-windows-net")
    
    # Upload the local file to the blob
    wasb_hook.load_file(LOCAL_DESTINATION_MC, container_name=CONTAINER_NAME_MC, blob_name=BLOB_NAME_MC, overwrite=True)
    
    logging.info(f'Uploaded {LOCAL_DESTINATION_MC} to Azure Blob Storage as {BLOB_NAME_MC}')

def pull_csga_from_sql(**kwargs):
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
   
    
    dfCSGCat = sql_hook.get_pandas_df(query)
    logging.info(f'Query executed successfully, retrieved {len(dfCSGCat)} records.')

    # Save DataFrame to CSV locally
    dfCSGCat.to_parquet(LOCAL_DESTINATION_CSG, index=False)
    logging.info(f'Data saved locally to {LOCAL_DESTINATION_CSG}')


drop_csg_table = KonzaTrinoOperator(
    task_id='drop_csg_trino_table',
    query="DROP TABLE IF EXISTS hive.parquet_master_data.client_security_groupings_trino"
)
create_csg_table = KonzaTrinoOperator(
    task_id='create_csg_trino_table',
    query="""
        CREATE TABLE hive.parquet_master_data.client_security_groupings_trino (
            client varchar,
            mpi varchar,
            provider_npi varchar,
            provider_db_id varchar,
            provider_fname varchar,
            provider_lname varchar,
            server_id varchar, 
            visits_found_within_36_months varchar,
            event_timestamp varchar
        )
        WITH (
    external_location = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/client_security_groupings_trino',
    format = 'PARQUET'
)
    """
)

def upload_csg_to_azure_blob(**kwargs):
    # Create a WasbHook object
    wasb_hook = WasbHook(wasb_conn_id="reportwriterstorage-blob-core-windows-net")
    
    # Upload the local file to the blob
    wasb_hook.load_file(LOCAL_DESTINATION_CSG, container_name=CONTAINER_NAME_CSG, blob_name=BLOB_NAME_CSG, overwrite=True)
    
    logging.info(f'Uploaded {LOCAL_DESTINATION_CSG} to Azure Blob Storage as {BLOB_NAME_CSG}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="upload_latest_mc_and_csga_parquet",
    default_args=default_args,
    schedule_interval=None,
    tags=['C-171']
) as dag:
    
    task_pull_mc_data = PythonOperator(
        task_id="pull_mc_data",
        python_callable=pull_mc_from_sql,
        provide_context=True,
    )
    
    task_upload_blob = PythonOperator(
        task_id="upload_to_azure_blob",
        python_callable=upload_to_azure_blob,
        provide_context=True,
    )

    task_pull_csga_data = PythonOperator(
        task_id="pull_csga_data",
        python_callable=pull_csga_from_sql,
        provide_context=True,
    )

    task_upload_csg_blob = PythonOperator(
        task_id="upload_csg_to_azure_blob",
        python_callable=upload_csg_to_azure_blob,
        provide_context=True,
    )
    
    task_pull_mc_data >> drop_mc_table >> create_mc_table >> task_upload_blob >> task_pull_csga_data >> drop_csg_table >> create_csg_table >> task_upload_csg_blob # Set task dependencies