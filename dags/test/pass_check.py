from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime

def get_conn_password(conn_id: str, **kwargs):
    conn = BaseHook.get_connection(conn_id)
    password = conn.password
    return password  # This will be pushed to XCom automatically

with DAG(
    dag_id='retrieve_conn_password',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Airflow 3.x uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=['example'],
    params={
        'conn_id': 'your_default_conn_id_here'
    }
) as dag:

    retrieve_password = PythonOperator(
        task_id='get_password',
        python_callable=get_conn_password,
        op_args=["{{ params.conn_id }}"],
    )
