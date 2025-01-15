from airflow.hooks.base import BaseHook
import trino
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
def execute_trino_queries(**kwargs):
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

    # Define the SQL queries
    queries = [
        """CREATE TABLE IF NOT EXISTS hive.parquet_master_data.patient_account_parquet_pm_by_accid ( admitted varchar, source varchar, unit_id varchar, related_provider_id varchar, accid varchar, index_update varchar ) WITH (partitioned_by = ARRAY['index_update'], bucketed_by = ARRAY['accid'], bucket_count = 64 )""",
        f"""
        INSERT INTO hive.parquet_master_data.patient_account_parquet_pm_by_accid
        SELECT
        admitted ,
        source , 
        unit_id , 
        related_provider_id,
        accid,
        index_update
        FROM patient_account_parquet_pm
        WHERE concat(index_update,'-01') = '{ds}'
        """,
        f"""
        CREATE TABLE test_dag_based_patient_by_acc_id_sorted_3 AS
        SELECT
    accid,
    MAX_BY(
        index_update,
        CAST(ROW(admitted, source, unit_id, related_provider_id) AS ROW(admitted VARCHAR, source VARCHAR, unit_id VARCHAR, related_provider_id VARCHAR))
    ) AS latest_known
        FROM hive.parquet_master_data.patient_account_parquet_pm_by_accid
        GROUP BY accid
        """
    ]
    
    # Execute each query
    for query in queries:
        cursor.execute(query)
        print(f"Executed query: {query}")

    cursor.close()
    trino_conn.close()

with DAG(
    dag_id='bucket_master_patient_index',
    schedule_interval='@monthly',
    max_active_runs=1,
    tags=['C-35'],
    start_date=datetime(2023, 3, 1),
    catchup=True,
) as dag:

    execute_queries_task = PythonOperator(
        task_id='execute_trino_queries',
        python_callable=execute_trino_queries,
        provide_context=True,
    )
    execute_queries_task
