from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime

def copy_table_in_chunks():
    source_conn = MySqlHook(mysql_conn_id='prd-az1-sqlw2-airflowconnection')
    dest_conn = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')

    chunk_size = 10000  # Adjust the chunk size as needed
    offset = 0

    while True:
        rows = source_conn.get_records(f"""
            SELECT * FROM temp.pa_thirtysix_month_lookback
            LIMIT {chunk_size} OFFSET {offset};
        """)

        if not rows:
            break

        dest_conn.insert_rows('temp.pa_thirtysix_month_lookback', rows)
        offset += chunk_size

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 31),
    'retries': 1,
}

with DAG('copy_table_dag', default_args=default_args, schedule_interval='@daily') as dag:
    copy_task = PythonOperator(
        task_id='copy_table_in_chunks',
        python_callable=copy_table_in_chunks
    )
