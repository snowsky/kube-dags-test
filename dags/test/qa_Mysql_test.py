import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime

def check_mysql_connection():
    connection = BaseHook.get_connection('qa-az1-sqlw3-airflowconnection')
    try:
        conn = connection.get_hook().get_conn()
        conn.ping()
        logging.info("Connection successful!")
    except Exception as e:
        logging.error(f"Connection failed: {e}")

def run_mysql_query():
    connection = BaseHook.get_connection('qa-az1-sqlw3-airflowconnection')
    try:
        conn = connection.get_hook().get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM client_hierarchy")
        results = cursor.fetchall()
        for row in results:
            logging.info(row)
    except Exception as e:
        logging.error(f"Query failed: {e}")

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

    run_query_task = PythonOperator(
        task_id='run_mysql_query',
        python_callable=run_mysql_query
    )

    check_connection_task >> run_query_task
