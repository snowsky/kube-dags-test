from airflow.hooks.base import BaseHook
import trino
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


class KonzaTrinoOperator(PythonOperator):

    def __init__(self, query, **kwargs):

        def execute_trino_query(**kwargs):
            ds = kwargs['ds']
            # Retrieve the connection details
            conn = BaseHook.get_connection('trinokonza')
            host = conn.host
            port = conn.port
            user = conn.login
            schema = conn.schema

            # Connect to Trino
            trino_conn = trino.dbapi.connect(
                host=host,
                port=port,
                user=user,
                catalog='hive',
                schema=schema,
            )
            cursor = trino_conn.cursor()

            # the .replace is a no-op if ds not present in query
            cursor.execute(query.replace('<DATEID>', ds))
            print(f"Executed query: {query}")

            cursor.close()
            trino_conn.close()

        super(KonzaTrinoOperator, self).__init__(
            python_callable=execute_trino_query,
            provide_context=True,
            **kwargs
        )

with DAG(
    dag_id='EDW_State_Report',
    schedule_interval='@monthly',
    tags=['C-111'],
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
) as dag:
    drop_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='drop_accid_by_state_prep__final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
        """,
    )
    create_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='create_accid_by_state_prep__final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
        ( patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar)
        """,
    )
    insert_accid_by_state_prep__final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_prep__final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_prep__final
      (select patient_id, index_update_dt_tm, state from patient_contact_parquet_pm)
        """,
    )
    drop_accid_by_state_final = KonzaTrinoOperator(
        task_id='drop_accid_by_state_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_final
        """,
    )
    create_accid_by_state_final = KonzaTrinoOperator(
        task_id='create_accid_by_state_final',
        query="""
              CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_by_state_final
      ( patient_id varchar, 
      index_update_dt_tm varchar, 
      state varchar)
        """,
    )
    insert_accid_by_state_final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_final
        (select * from sup_12760_c59_accid_by_state_prep__final where state <> '')
        """,
    )
    drop_accid_by_state_final = KonzaTrinoOperator(
        task_id='drop_accid_by_state_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_by_state_distinct__final
        """,
    )
    create_accid_by_state_distinct__final = KonzaTrinoOperator(
        task_id='create_accid_by_state_distinct__final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_by_state_distinct__final
        ( patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar)
        """,
    )
    insert_accid_by_state_distinct__final = KonzaTrinoOperator(
        task_id='insert_accid_by_state_distinct__final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_by_state_distinct__final
      (select distinct * from hive.parquet_master_data.sup_12760_c59_accid_by_state_final)
        """,
    )
    drop_accid_state_distinct_rank_final = KonzaTrinoOperator(
        task_id='drop_accid_state_distinct_rank_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_final
        """,
    )
    create_accid_state_distinct_rank_final = KonzaTrinoOperator(
        task_id='create_accid_state_distinct_rank_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_final
(rank bigint,
patient_id varchar, 
index_update_dt_tm varchar, 
state varchar)
        """,
    )
    insert_accid_state_distinct_rank_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_final
    (select DENSE_RANK() OVER(Partition by T.patient_id ORDER BY T.index_update_dt_tm DESC) as rank,* from sup_12760_c59_accid_by_state_distinct__final T)
        """,
    )
    drop_accid_state_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='drop_accid_state_distinct_rank_1_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_final
        """,
    )
    create_accid_state_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='create_accid_state_distinct_rank_1_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_final
        (rank bigint,
        patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar)
        """,
    )
    insert_accid_state_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_1_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_final
(select * from sup_12760_c59_accid_state_distinct_rank_final where rank = 1)
        """,
    )
    drop_mpi_accid_prep_final = KonzaTrinoOperator(
        task_id='drop_mpi_accid_prep_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final
        """,
    )
    ## Seems like this is not used initially
    create_mpi_accid_prep_final = KonzaTrinoOperator(
        task_id='create_mpi_accid_prep_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final
        (mpi varchar, 
        accid_ref varchar, 
        index_update varchar
        WITH ( 
        partition_projection_format = 'yyyy-MM', 
        partition_projection_interval = 1, 
        partition_projection_interval_unit = 'DAYS', 
        partition_projection_range = ARRAY['2023-03','2024-10'], --'NOW' --can be used if in the current month EG. if there is no month folder for 2024-08, and it is August, must set a range to the prior month
        partition_projection_type = 'DATE' ) ) WITH 
        ( external_location = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/mpi',
        format = 'PARQUET', partition_projection_enabled = true, 
        partition_projection_location_template = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/mpi/${index_update}', 
        partitioned_by = ARRAY['index_update'] )
        """,
    )
    drop_mpi_accid_no_blanks = KonzaTrinoOperator(
        task_id='drop_mpi_accid_no_blanks',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
        """,
    )
    create_mpi_accid_no_blanks = KonzaTrinoOperator(
        task_id='create_mpi_accid_no_blanks',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
        (mpi varchar, 
        accid_ref varchar, 
        index_update varchar)
        """,
    )
    insert_mpi_accid_no_blanks = KonzaTrinoOperator(
        task_id='insert_mpi_accid_no_blanks',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks
        (select * from hive.parquet_master_data.sup_12760_c59_mpi_accid_prep_final
        where mpi <> ''
        and accid_ref <> ''
        and accid_ref <> 'None'
        and index_update <> '')
        """,
    )
    drop_mpi_accid_final = KonzaTrinoOperator(
        task_id='drop_mpi_accid_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_accid_final)
        """,
    )
    create_mpi_accid_final = KonzaTrinoOperator(
        task_id='create_mpi_accid_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_accid_final
        (accid_ref varchar, 
        mpi varchar)
        """,
    )
    insert_mpi_accid_final = KonzaTrinoOperator(
        task_id='insert_mpi_accid_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_accid_final
    (select accid_ref, mpi from hive.parquet_master_data.sup_12760_c59_mpi_accid_no_blanks)
        """,
    )
    drop_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='drop_accid_state_distinct_rank_1_mpi_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        """,
    )
    create_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='create_accid_state_distinct_rank_1_mpi_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        (rank bigint,
        patient_id varchar, 
        index_update_dt_tm varchar, 
        state varchar,
        mpi varchar)
        """,
    )
    insert_accid_state_distinct_rank_1_mpi_final = KonzaTrinoOperator(
        task_id='insert_accid_state_distinct_rank_1_mpi_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_accid_state_distinct_rank_1_mpi_final
        (select ST.*, MPI.mpi from sup_12760_c59_accid_state_distinct_rank_1_final ST
        JOIN sup_12760_c59_mpi_accid_final MPI on ST.patient_id = MPI.accid_ref)
        """,
    )
    drop_mpi_state_index = KonzaTrinoOperator(
        task_id='drop_mpi_state_index',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index
        """,
    )
    create_mpi_state_index = KonzaTrinoOperator(
        task_id='create_mpi_state_index',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index
        (mpi varchar,
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index = KonzaTrinoOperator(
        task_id='insert_mpi_state_index',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index
        (select mpi, state, index_update_dt_tm from sup_12760_c59_accid_state_distinct_rank_1_mpi_final)
        """,
    )
    drop_mpi_state_index_distinct_final = KonzaTrinoOperator(
        task_id='drop_mpi_state_index_distinct_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_final
        """,
    )
    create_mpi_state_index_distinct_final = KonzaTrinoOperator(
        task_id='create_mpi_state_index_distinct_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_final
        (mpi varchar,
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index_distinct_final = KonzaTrinoOperator(
        task_id='insert_mpi_state_index_distinct_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_final
(select DISTINCT * from sup_12760_c59_mpi_state_index)
        """,
    )
    drop_mpi_state_index_distinct_rank_final = KonzaTrinoOperator(
        task_id='drop_mpi_state_index_distinct_rank_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_final
        """,
    )
    create_mpi_state_index_distinct_rank_final = KonzaTrinoOperator(
        task_id='create_mpi_state_index_distinct_rank_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_final
        (rank_per_mpi bigint,
        mpi varchar, 
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index_distinct_rank_final = KonzaTrinoOperator(
        task_id='insert_mpi_state_index_distinct_rank_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_final
        (select row_number() OVER(Partition by T.mpi ORDER BY T.index_update_dt_tm DESC) as rank_per_mpi,* from sup_12760_c59_mpi_state_index_distinct_final T)
        """,
    )
    drop_mpi_state_index_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='drop_mpi_state_index_distinct_rank_1_final',
        query="""
        DROP TABLE IF EXISTS hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_1_final
        """,
    )
    create_mpi_state_index_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='create_mpi_state_index_distinct_rank_1_final',
        query="""
        CREATE TABLE hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_1_final
        (rank_per_mpi bigint,
        mpi varchar, 
        state varchar, 
        index_update_dt_tm varchar)
        """,
    )
    insert_mpi_state_index_distinct_rank_1_final = KonzaTrinoOperator(
        task_id='insert_mpi_state_index_distinct_rank_1_final',
        query="""
        INSERT INTO hive.parquet_master_data.sup_12760_c59_mpi_state_index_distinct_rank_1_final
        (select * from sup_12760_c59_mpi_state_index_distinct_rank_final where rank_per_mpi = 1)
        """,
    )
    drop_accid_by_state_prep__final >> create_accid_by_state_prep__final >> insert_accid_by_state_prep__final >> drop_accid_by_state_final >> create_accid_by_state_final >> insert_accid_by_state_final >> drop_accid_by_state_final >> create_accid_by_state_distinct__final >> insert_accid_by_state_distinct__final >> drop_accid_state_distinct_rank_final >> create_accid_state_distinct_rank_final >> insert_accid_state_distinct_rank_final >> drop_accid_state_distinct_rank_1_final >> create_accid_state_distinct_rank_1_final >> insert_accid_state_distinct_rank_1_final >> drop_mpi_accid_prep_final >> create_mpi_accid_prep_final >> drop_mpi_accid_no_blanks >> create_mpi_accid_no_blanks >> insert_mpi_accid_no_blanks >> drop_mpi_accid_final >> create_mpi_accid_final >> insert_mpi_accid_final >> drop_accid_state_distinct_rank_1_mpi_final >> create_accid_state_distinct_rank_1_mpi_final >> insert_accid_state_distinct_rank_1_mpi_final >> drop_mpi_state_index >> create_mpi_state_index >> insert_mpi_state_index >> drop_mpi_state_index_distinct_final >> create_mpi_state_index_distinct_final >> insert_mpi_state_index_distinct_final >> drop_mpi_state_index_distinct_rank_final >> create_mpi_state_index_distinct_rank_final >> insert_mpi_state_index_distinct_rank_final >> drop_mpi_state_index_distinct_rank_1_final >> create_mpi_state_index_distinct_rank_1_final >> insert_mpi_state_index_distinct_rank_1_final
