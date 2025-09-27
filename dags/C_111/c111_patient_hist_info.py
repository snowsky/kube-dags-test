"""
c111_patient_hist_info airflow DAG.

This Airflow DAG produces the c111_patient_hist_info table,
which consolidates certain values from historical partitions of patient_contact_parquet_pm. 
"""
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/dags')

import time
import trino
import logging
import mysql.connector
from datetime import datetime, timedelta, timezone
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from lib.operators.konza_trino_operator import KonzaTrinoOperator
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from lib.operators.aks_trino_kubernetes import scale_trino_workers

default_args = {
    'owner': 'airflow',
    'retries': 2,  # Set the number of retries to 2
    'retry_delay': timedelta(minutes=5)  # Optional: Set the delay between retries
}

with DAG(
    dag_id='c111_patient_hist_info',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    tags=['C-111'],
    max_active_runs=1,
    catchup=False,
) as dag:

    create_c111_patient_hist_info = KonzaTrinoOperator(
        task_id='create_c111_patient_hist_info',
        query="""
        CREATE TABLE IF NOT EXISTS hive.parquet_master_data.c111_patient_hist_info
        (     
            patient_id VARCHAR, 
            index_update_dt_tm VARCHAR, 
            state VARCHAR 
        ) 
        COMMENT '''[C-111] Historical table consolidating patient_contact_parquet_pm up to June 2024. 

        patient_contact_parquet_pm has too many partitions so this table is used to consolidate all the partitions up to 2024-06.
        '''
        """,
    )

    populate_c111_patient_hist_info = KonzaTrinoOperator(
        task_id='populate_c111_patient_hist_info',
        query="""
        INSERT INTO hive.parquet_master_data.c111_patient_hist_info
        SELECT
            patient_id, 
            index_update_dt_tm, 
            state
        FROM patient_contact_parquet_pm s 
        -- TODO: this should be checked
        WHERE index_update <= '2024-06'
        """,
    )

    create_c111_patient_hist_info >> populate_c111_patient_hist_info 
