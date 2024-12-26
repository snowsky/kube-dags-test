from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 19),
    'retries': 1,
}

with DAG('Connection_test_trino_dag', default_args=default_args, schedule='@daily', tags=['C-35']) as dag:
    trino_task = TrinoOperator(
        task_id='run_trino_query',
        sql='SELECT "schema_name" FROM "information_schema"."schemata";',
        conn_id='trinokonza'
    )
