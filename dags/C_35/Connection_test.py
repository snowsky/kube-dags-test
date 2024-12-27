from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime
import subprocess

# hardcode dependencies
def install_dependencies():
    subprocess.check_call(["pip", "install", "apache-airflow"])
    subprocess.check_call(["pip", "install", "apache-airflow-providers-common-sql"])
    subprocess.check_call(["pip", "install", "pandas"])
    subprocess.check_call(["pip", "install", "trino"])
    subprocess.check_call(["pip", "install", "pymssql"])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 19),
    'retries': 1,
}

# install dependencies
install_dependencies()

with DAG('Connection_test_trino_dag', default_args=default_args, schedule='@daily', tags=['C-35']) as dag:
    trino_task = TrinoOperator(
        task_id='run_trino_query',
        sql='SELECT "schema_name" FROM "information_schema"."schemata";',
        conn_id='trinokonza'
    )
