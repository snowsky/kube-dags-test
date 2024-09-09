from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime

def check_mysql_connection():
    connection = BaseHook.get_connection('qa-az1-sqlw3-airflowconnection')
    try:
        conn = connection.get_hook().get_conn()
        conn.ping()
        print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 9),
    'retries': 1,
}

with DAG('check_mysql_connection_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    check_connection_task = PythonOperator(
        task_id='check_mysql_connection',
        python_callable=check_mysql_connection
    )

    check_connection_task
