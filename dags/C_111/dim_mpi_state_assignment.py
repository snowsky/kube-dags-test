"""
dim_mpi_state_assignment Airflow DAG.

This pipeline joins two tables together:
- dim_accid_state_assignment_latest (produced by the dim_accid_state_assignment DAG), and
- dim_accid_to_mpi (produced by the dim_accid_to_mpi DAG)

The product of this pipeline is the table called dim_mpi_state_assignment. This table is
keyed on the mpi column and records one state assignment for each MPI.
"""

import time
import trino
import logging
import mysql.connector
from datetime import datetime, timedelta, timezone
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from lib.operators.konza_trino_operator import KonzaTrinoOperator
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 2,  # Set the number of retries to 2
    'retry_delay': timedelta(minutes=5)  # Optional: Set the delay between retries
}

with DAG(
    dag_id='dim_mpi_state_assignment',
    schedule_interval='@monthly',
    tags=['C-111'],
    start_date=datetime(2025, 3, 1),
    catchup=True,
    max_active_runs=1,
) as dag:

    create_dim_accid_state_assignment_latest_with_mpi = KonzaTrinoOperator(
        task_id='create_dim_accid_state_assignment_latest_with_mpi',
        query="""
        CREATE TABLE IF NOT EXISTS hive.parquet_master_data.dim_accid_state_assignment_latest_with_mpi
        (     
            patient_id VARCHAR, 
            latest_index_update_dt_tm VARCHAR, 
            used_index_update_dt_tm VARCHAR, 
            imputed_state VARCHAR, 
            used_algorithm VARCHAR,
            mpi VARCHAR,
            ds VARCHAR
        ) 
        COMMENT '''[C-111] State assignment for every patient with their MPI. 
        One row per patient_id. Note that multiple patient_ids may map to the same MPI.
        '''
        WITH (
            partitioned_by = ARRAY['ds'], 
            bucketed_by = ARRAY['patient_id'], 
            sorted_by = ARRAY['patient_id'],
            bucket_count = 64
        )
        """,
    )

    dim_accid_state_assignment_latest = ExternalTaskMarker(
        task_id="dim_accid_state_assignment_latest",
        external_dag_id="dim_accid_state_assignment",
        external_task_id="populate_dim_accid_state_assignment_latest",
    )
    
    dim_accid_to_mpi = ExternalTaskMarker(
        task_id="dim_accid_to_mpi",
        external_dag_id="dim_accid_to_mpi",
        external_task_id="populate_dim_accid_to_mpi",
    )

    populate_dim_accid_state_assignment_latest_with_mpi = KonzaTrinoOperator(
        task_id='populate_dim_accid_state_assignment_latest_with_mpi',
        query="""
        INSERT INTO hive.parquet_master_data.dim_accid_state_assignment_latest_with_mpi
        SELECT 
          patient_id, 
          latest_index_update_dt_tm, 
          used_index_update_dt_tm, 
          imputed_state, 
          used_algorithm, 
          mpi,
          '<DATEID>' AS ds
        FROM hive.parquet_master_data.dim_accid_state_assignment_latest t1
        LEFT JOIN hive.parquet_master_data.dim_accid_to_mpi t2
        ON t1.patient_id = t2.accid_ref
        WHERE t1.ds = '<DATEID>'
        AND t2.ds = '<DATEID>'
        """,
    )

    create_dim_mpi_state_assignment = KonzaTrinoOperator(
        task_id='create_dim_mpi_state_assignment',
        query="""
        CREATE TABLE IF NOT EXISTS hive.parquet_master_data.dim_mpi_state_assignment
        (     
            mpi VARCHAR,
            latest_index_update_dt_tm VARCHAR, 
            used_index_update_dt_tm VARCHAR, 
            imputed_state VARCHAR, 
            used_algorithm VARCHAR,
            patient_id_used_for_assignment VARCHAR,
            number_patient_ids BIGINT,
            ds VARCHAR
        ) 
        COMMENT '''
        [C-111] State assignment for each MPI. 

        One row per MPI. Note that multiple patient_ids may map to the same MPI.
        If multiple patient_ids map to the same MPI, the latest valid state assignment 
        will be used, according to the index update. If multiple valid assignments
        come from the same index update, one such assignment will be picked at random.
        '''
        WITH (
            partitioned_by = ARRAY['ds'], 
            bucketed_by = ARRAY['mpi'], 
            sorted_by = ARRAY['mpi'],
            bucket_count = 64
        )
        """,
    )
    populate_dim_mpi_state_assignment = KonzaTrinoOperator(
        task_id='populate_dim_mpi_state_assignment',
        query="""
        INSERT INTO hive.parquet_master_data.dim_mpi_state_assignment
        SELECT 
            mpi,
            assignment.latest_index_update_dt_tm AS latest_index_update_dt_tm,
            assignment.used_index_update_dt_tm AS used_index_update_dt_tm,
            assignment.imputed_state AS imputed_state,
            assignment.used_algorithm AS used_algorithm,
            assignment.patient_id AS patient_id_used_for_assignment,
            number_patient_ids,
            '<DATEID>' AS ds
        FROM (
            SELECT 
                mpi, 
                MAX_BY(
                    CAST(
                        ROW(latest_index_update_dt_tm, used_index_update_dt_tm, imputed_state, used_algorithm, patient_id)
                        AS ROW(
                            latest_index_update_dt_tm VARCHAR,
                            used_index_update_dt_tm VARCHAR,
                            imputed_state VARCHAR,
                            used_algorithm VARCHAR,
                            patient_id VARCHAR
                        )
                    ),
                    used_index_update_dt_tm
                ) AS assignment,
                COUNT(*) AS number_patient_ids
            FROM hive.parquet_master_data.dim_accid_state_assignment_latest_with_mpi
            WHERE ds = '<DATEID>'
            GROUP BY mpi
        )
        """,
    )

    create_agg_state_assignment_count = KonzaTrinoOperator(
        task_id='create_agg_state_assignment_count',
        query="""
        CREATE TABLE IF NOT EXISTS hive.parquet_master_data.agg_state_assignment_count
        (     
            imputed_state VARCHAR, 
            used_algorithm VARCHAR,
            number_mpis BIGINT,
            number_patient_ids BIGINT,
            ds VARCHAR
        ) 
        COMMENT '[C-111] Distinct MPI and patient ID counts per imputed state and used algorithm.'
        WITH (
            partitioned_by = ARRAY['ds']
        )
        """,
    )
    populate_agg_state_assignment_count = KonzaTrinoOperator(
        task_id='populate_agg_state_assignment_count',
        query="""
        INSERT INTO hive.parquet_master_data.agg_state_assignment_count
        SELECT 
          imputed_state,
          used_algorithm,
          COUNT(*) AS number_mpis,
          SUM(number_patient_ids) AS number_patient_ids
        FROM hive.parquet_master_data.dim_mpi_state_assignment
        GROUP BY imputed_state, used_algorithm
        """,
    )
    

    dim_accid_to_mpi >> populate_dim_accid_state_assignment_latest_with_mpi
    dim_accid_state_assignment_latest >> populate_dim_accid_state_assignment_latest_with_mpi
    create_dim_accid_state_assignment_latest_with_mpi >> populate_dim_accid_state_assignment_latest_with_mpi
    populate_dim_accid_state_assignment_latest_with_mpi >> populate_dim_mpi_state_assignment
    create_dim_mpi_state_assignment >> populate_dim_mpi_state_assignment
    create_agg_state_assignment_count >> populate_agg_state_assignment_count 
    populate_dim_mpi_state_assignment >> populate_agg_state_assignment_count

