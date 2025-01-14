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
    dag_id='thirty_six_month_1589',
    schedule_interval='@monthly',
    tags=['C-35'],
    start_date=datetime(2023, 3, 1),
    catchup=True,
    max_active_runs=1,
) as dag:
    create_patient_account_by_acc_id_table = KonzaTrinoOperator(
        task_id='create_patient_account_by_acc_id_table',
        query="""
        CREATE TABLE IF NOT EXISTS 
        hive.parquet_master_data.patient_account_parquet_pm_by_accid ( 
            admitted VARCHAR, 
            source VARCHAR, 
            unit_id VARCHAR, 
            related_provider_id VARCHAR, 
            accid VARCHAR, 
            index_update VARCHAR 
        ) WITH (
            partitioned_by = ARRAY['index_update'], 
            bucketed_by = ARRAY['accid'], 
            sorted_by = ARRAY['accid'],
            bucket_count = 64 
        )
        """,
    )

    bucket_patient_account_by_acc_id_table = KonzaTrinoOperator(
        task_id='bucket_patient_account_by_acc_id_table',
        query="""
        INSERT INTO parquet_master_data.patient_account_parquet_pm_by_accid
        SELECT
            admitted,
            source, 
            unit_id, 
            related_provider_id,
            accid,
            index_update
        FROM patient_account_parquet_pm
        WHERE concat(index_update,'-01') = '<DATEID>'
        """
     )
    create_mpi_parquet_pm_by_acc_id_table = KonzaTrinoOperator(
        task_id='create_mpi_parquet_pm_by_acc_id_table',
        query="""
        CREATE TABLE IF NOT EXISTS 
        hive.parquet_master_data.mpi_parquet_pm_by_accid (
    index_update_dt_tm varchar,
 source varchar, 
 source_type varchar, 
 mpi varchar, 
 mpi_update varchar, 
 vault_pid varchar, 
 id varchar, 
 mrn varchar, 
 prefix varchar, 
 firstname varchar, 
 middlename varchar, 
 lastname varchar, 
 suffix varchar, 
 dob varchar, 
 sex varchar, 
 race varchar, 
 ethnicity varchar, 
 citizenship varchar, 
 nationality varchar, 
 language varchar, 
 religion varchar, 
 marital_status varchar, 
 mil_status varchar, 
 student_status varchar, 
 hipaa varchar, 
 deceased varchar, 
 mother_maiden_name varchar, 
 event_timestamp varchar, 
 created varchar, 
 updated varchar, 
 collect_registered varchar, 
 collect_cmp varchar, 
 stable_document_id varchar, 
 ssn varchar, 
 dlnum varchar, 
 dlstate varchar, 
 alt_id_name varchar, 
 alt_id_num varchar, 
 policy_id varchar, 
 discharge_status_code varchar, 
 discharge_status_description varchar, 
 accid_ref varchar, 
            index_update VARCHAR 
        ) WITH (
            -- I assume index_update also makes sense here 
            external_location = 'abfs://content@reportwriterstorage.dfs.core.windows.net/parquet-master-data/mpi_parquet_pm_by_accid',
            partitioned_by = ARRAY['index_update'], 
            bucketed_by = ARRAY['accid_ref'], 
            bucket_count = 64 
        )
        """,
    )

    bucket_mpi_parquet_pm_by_acc_id_table = KonzaTrinoOperator(
        task_id='bucket_mpi_parquet_pm_by_acc_id_table',
        query="""
        INSERT INTO parquet_master_data.mpi_parquet_pm_by_accid
        SELECT
        -- @biakonza, please check this makes sense
index_update_dt_tm ,
 source , 
 source_type , 
 mpi , 
 mpi_update , 
 vault_pid , 
 id , 
 mrn , 
 prefix , 
 firstname , 
 middlename , 
 lastname , 
 suffix , 
 dob , 
 sex , 
 race , 
 ethnicity , 
 citizenship , 
 nationality , 
 language , 
 religion , 
 marital_status , 
 mil_status , 
 student_status , 
 hipaa , 
 deceased , 
 mother_maiden_name , 
 event_timestamp , 
 created , 
 updated , 
 collect_registered , 
 collect_cmp , 
 stable_document_id , 
 ssn , 
 dlnum , 
 dlstate , 
 alt_id_name , 
 alt_id_num , 
 policy_id , 
 discharge_status_code , 
 discharge_status_description , 
 accid_ref , 
            index_update
        FROM mpi_parquet_pm
        WHERE concat(index_update,'-01') = '<DATEID>'
        """
    )
    create_patient_account_latest_past36months_table = KonzaTrinoOperator(
        task_id='create_patient_account_latest_past36months_table',
        query="""
        CREATE TABLE IF NOT EXISTS 
        hive.parquet_master_data.patient_account_latest_past36months ( 
            accid VARCHAR, 
            latest_known_admitted VARCHAR, 
            latest_known_source VARCHAR, 
            latest_known_unit_id VARCHAR, 
            latest_known_related_provider_id VARCHAR, 
            latest_index_update VARCHAR,
            -- note the use of ds here as the partitioning column
            -- ds is the date w/r to which the 36-month lookback is computed!
            ds VARCHAR
        ) WITH (
            partitioned_by = ARRAY['ds'], 
            bucketed_by = ARRAY['accid'], 
            sorted_by = ARRAY['accid'],
            bucket_count = 64 
        )
        """,
    )

    populate_patient_account_latest_past36months_table = KonzaTrinoOperator(
        task_id='populate_patient_account_latest_past36months_table',
        query="""
        INSERT INTO parquet_master_data.patient_account_latest_past36months
SELECT 
  CAST(accid AS varchar) AS accid,
  CAST(latest_known_admitted AS varchar) AS latest_known_admitted,
  CAST(latest_known_source AS varchar) AS latest_known_source,
  CAST(latest_known_unit_id AS varchar) AS latest_known_unit_id,
  CAST(latest_known_related_provider_id AS varchar) AS latest_known_related_provider_id,
  CAST('<DATEID>' AS varchar) AS ds,
  CAST('' AS varchar) AS mpi -- Add a placeholder column if needed
FROM (
  SELECT
    accid,
    admitted AS latest_known_admitted,
    source AS latest_known_source,
    unit_id AS latest_known_unit_id,
    related_provider_id AS latest_known_related_provider_id,
    ROW_NUMBER() OVER (PARTITION BY accid ORDER BY CAST(DATE_PARSE(index_update || '-01', '%Y-%m-%d') AS date) DESC) AS row_num
  FROM hive.parquet_master_data.patient_account_parquet_pm_by_accid
  WHERE CAST(DATE_PARSE(index_update || '-01', '%Y-%m-%d') AS date) >= DATE_ADD('month', -36, CAST('<DATEID>' AS date))
  AND admitted <> '1900-00-00 00:00:00' AND admitted <> '0000-00-00 00:00:00' 
  AND CAST(admitted AS timestamp) >= DATE_ADD('month', -36, CAST('<DATEID>' AS date))
  
) subquery
WHERE row_num = 1
        """,
    )
    create_patient_account_latest_past36months_mpi_pm_table = KonzaTrinoOperator(
        task_id='create_patient_account_latest_past36months_mpi_pm_table',
        query="""
        CREATE TABLE IF NOT EXISTS 
        hive.parquet_master_data.patient_account_latest_past36months_mpi_pm ( 
            accid VARCHAR, 
            latest_known_admitted VARCHAR, 
            latest_known_source VARCHAR, 
            latest_known_unit_id VARCHAR, 
            latest_known_related_provider_id VARCHAR, 
            latest_index_update VARCHAR,
            MPI VARCHAR,
            ds VARCHAR
        ) WITH (
            partitioned_by = ARRAY['ds']
        )
        """,
    )
    populate_patient_account_latest_past36months_mpi_pm_table = KonzaTrinoOperator(
        task_id='populate_patient_account_latest_past36months_mpi_pm_table',
        query="""
                INSERT INTO
            parquet_master_data.patient_account_latest_past36months_mpi_pm
        SELECT 
          t1.accid,
          latest_known_admitted, 
          latest_known_source,
          latest_known_unit_id,
          latest_known_related_provider_id,
          latest_index_update,
          CAST(MPI AS VARCHAR),  -- Ensure MPI is cast to VARCHAR
          CAST('<DATEID>' AS VARCHAR) AS ds  -- Cast ds as general VARCHAR
        FROM
            parquet_master_data.patient_account_latest_past36months t1
        JOIN
            parquet_master_data.mpi_parquet_pm_by_accid t2
        ON t1.accid = t2.accid_ref
        -- assumes both tables have a column called accid
        --USING (accid)
        WHERE t1.ds = '<DATEID>'
        -- @biakonza, there might be additional clauses you might whish to impose on
        -- mpi_parquet_pm_by_accid -- is there a lookback there too? -ET no there is not a lookback there too, index update does play the same replace with newer role.
        -- or does an index_update also play a role? etc.
        --- AND [...]
        """,
    )


    create_patient_account_by_acc_id_table >> bucket_patient_account_by_acc_id_table
    create_mpi_parquet_pm_by_acc_id_table >> bucket_mpi_parquet_pm_by_acc_id_table
    create_patient_account_latest_past36months_table >> populate_patient_account_latest_past36months_table
    bucket_patient_account_by_acc_id_table >> populate_patient_account_latest_past36months_table
    create_patient_account_latest_past36months_mpi_pm_table >> populate_patient_account_latest_past36months_mpi_pm_table
    populate_patient_account_latest_past36months_table >> populate_patient_account_latest_past36months_mpi_pm_table
    bucket_mpi_parquet_pm_by_acc_id_table >> populate_patient_account_latest_past36months_mpi_pm_table
