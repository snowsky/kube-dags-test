"""
dim_accid_to_mpi Airflow DAG

This DAG produces the dim_accid_to_mpi. Each ds of this table contains the totality of
known accid_ref's in sup_12760_c59_mpi_accid_prep_final_repartitioned at the time the pipeline
ran. The table is keyed on accid_ref.
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
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 2,  # Set the number of retries to 2
    'retry_delay': timedelta(minutes=5)  # Optional: Set the delay between retries
}

with DAG(
    dag_id='dim_acc_id_to_mpi',
    schedule_interval='@monthly',
    tags=['C-111'],
    start_date=datetime(2025, 3, 1),
    catchup=True,
    max_active_runs=1,
) as dag:
    
    create_dim_accid_to_mpi = KonzaTrinoOperator(
        task_id='create_dim_accid_to_mpi',
        query="""
        CREATE TABLE hive.parquet_master_data.dim_accid_to_mpi ( 
            accid_ref VARCHAR, 
            mpi VARCHAR,
            num_mpis_should_be_1 BIGINT,
            ds VARCHAR
        ) 
        COMMENT '[C-111] Latest known mapping of accid to MPI. Note that num_mpis_should_be_1 should be 1 for all values.'
        WITH ( 
          partitioned_by = ARRAY['ds'],
          bucket_count = 64, 
          bucketed_by = ARRAY['accid_ref'], 
          sorted_by = ARRAY['accid_ref']
        )
        """,
    )
    
    drop_tmp_dim_accid_to_mpi = KonzaTrinoOperator(
        task_id='drop_tmp_dim_accid_to_mpi',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.tmp_dim_accid_to_mpi
        """,
    )
    
    create_tmp_dim_accid_to_mpi = KonzaTrinoOperator(
        task_id='create_tmp_dim_accid_to_mpi',
        query="""
        CREATE TABLE hive.parquet_master_data.tmp_dim_accid_to_mpi
        COMMENT '''[C-111] Intermediary table for dim_accid_to_mpi. 
        Contains all known pairs of (accid_ref, mpi) across all partitions of 
        sup_12760_c59_mpi_accid_prep_final_repartitioned.
        This table is necessary to prevent Trino from crashing under load trying
        to query all the data in the source table while also grouping it. Note that 
        the table is only supposed to live for the duration of this pipeline.
        '''
        AS SELECT 
           accid_ref, 
           mpi
        FROM hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final_repartitioned
        """,
    )
    
    drop_tmp_dim_accid_to_mpi_grouped = KonzaTrinoOperator(
        task_id='drop_tmp_dim_accid_to_mpi_grouped',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.tmp_dim_accid_to_mpi_grouped
        """,
    )
    
    create_dim_accid_to_mpi_grouped = KonzaTrinoOperator(
        task_id='create_dim_accid_to_mpi_grouped',
        query="""
        CREATE TABLE hive.parquet_master_data.tmp_dim_accid_to_mpi_grouped
        COMMENT '''[C-111] Intermediary table for dim_accid_to_mpi.

        Computing this table requires 15-20 trino workers, which should already be scaled
        up by the task populating tmp_dim_accid_to_mpi in the dim_accid_to_mpi airflow pipeline.
        If these workers aren't available you may need to scale the cluster manually.
        '''
        WITH (format = 'PARQUET')
        AS SELECT 
           accid_ref, 
           ARBITRARY(mpi) as mpi,
           COUNT(DISTINCT mpi) AS num_mpis_should_be_1,
           '<DATEID>' AS ds
        FROM hive.parquet_master_data.tmp_dim_accid_to_mpi
        GROUP BY accid_ref
        """,
    )

    populate_dim_accid_to_mpi = KonzaTrinoOperator(
        task_id='populate_dim_accid_to_mpi',
        query="""
        INSERT INTO hive.parquet_master_data.dim_accid_to_mpi
        SELECT accid_ref, mpi, '<DATEID>' AS ds
        FROM hive.parquet_master_data.tmp_dim_accid_to_mpi_grouped
        """,
    )
    
    cleanup_tmp_dim_accid_to_mpi_grouped = KonzaTrinoOperator(
        task_id='cleanup_tmp_dim_accid_to_mpi_grouped',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.tmp_dim_accid_to_mpi_grouped
        """,
    )
    cleanup_tmp_dim_accid_to_mpi = KonzaTrinoOperator(
        task_id='cleanup_tmp_dim_accid_to_mpi',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.tmp_dim_accid_to_mpi
        """,
    )

    create_dim_accid_to_mpi >> populate_dim_accid_to_mpi
    drop_tmp_dim_accid_to_mpi >> create_tmp_dim_accid_to_mpi
    create_tmp_dim_accid_to_mpi >> create_dim_accid_to_mpi_grouped
    drop_tmp_dim_accid_to_mpi_grouped >> create_dim_accid_to_mpi_grouped
    create_dim_accid_to_mpi_grouped >> populate_dim_accid_to_mpi
    populate_dim_accid_to_mpi >> cleanup_tmp_dim_accid_to_mpi_grouped
    populate_dim_accid_to_mpi >> cleanup_tmp_dim_accid_to_mpi 
