from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from datetime import datetime

with DAG(
    dag_id='retrieve_conn_password',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
    params={
        'conn_id': 'your_default_conn_id_here'
    }
) as dag:

    @task
    def get_conn_password(conn_id: str):
        conn = BaseHook.get_connection(conn_id)
        return conn.password

    get_conn_password(conn_id="{{ params.conn_id }}")
