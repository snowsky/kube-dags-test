from airflow.hooks.base import BaseHook
import trino
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_trino_connection():
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
    cursor.execute('SELECT 1')
    result = cursor.fetchall()
    print(result)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import trino

def execute_trino_queries():
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
        "DROP TABLE IF EXISTS thirtysix_month_lookback",
        """
        CREATE TABLE thirtysix_month_lookback (
            admitted varchar,
            source varchar,
            unit_id varchar,
            related_provider_id varchar,
            accid varchar,
            partition_month varchar
        )
        WITH (
            format = 'PARQUET',
            partitioned_by = ARRAY['partition_month']
        )
        """
    ]

    # Add the INSERT queries for each month
    for i in range(36, 0, -1):
        queries.append(f"""
        INSERT INTO thirtysix_month_lookback
        SELECT
            admitted,
            source,
            unit_id,
            related_provider_id,
            accid,
            date_format(date_add('month', -{i}, current_date), '%Y-%m') as partition_month
        FROM patient_account_parquet_pm as PAM
        WHERE substr(cast(admitted as varchar), 1, 7) = date_format(date_add('month', -{i}, current_date), '%Y-%m')
        AND PAM.index_update in (date_format(date_add('month', -{i}, current_date), '%Y-%m'))
        """)

    # Add the DELETE query
    queries.append("""
    DELETE FROM thirtysix_month_lookback
    WHERE partition_month < date_format(date_add('month', -36, current_date), '%Y-%m')
    """)

    # Execute each query
    for query in queries:
        cursor.execute(query)
        print(f"Executed query: {query}")

    cursor.close()
    trino_conn.close()

with DAG(
    dag_id='execute_trino_queries_dag',
    schedule_interval='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    execute_queries_task = PythonOperator(
        task_id='execute_trino_queries',
        python_callable=execute_trino_queries,
    )
