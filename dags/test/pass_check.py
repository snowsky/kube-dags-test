from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

def get_conn_password(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    password = conn.password
    print(f"Password for connection '{conn_id}': {password}")
    return password

with DAG(
    dag_id='retrieve_conn_password',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
) as dag:

    retrieve_password = PythonOperator(
        task_id='get_password',
        python_callable=get_conn_password,
        op_args=['my_conn_id'],  # Replace with your actual conn_id
    )
