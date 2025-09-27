from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime

def create_table_if_not_exists():
    source_conn = MySqlHook(mysql_conn_id='prd-az1-sqlw2-airflowconnection')
    dest_conn = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')

    # Get the table schema from the source database
    table_schema = source_conn.get_records("""
        SELECT COLUMN_NAME, COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'pa_thirtysix_month_lookback'
        AND TABLE_SCHEMA = 'temp';
    """)

    # Construct the CREATE TABLE statement
    create_table_query = "CREATE TABLE IF NOT EXISTS temp.pa_thirtysix_month_lookback ("
    create_table_query += ", ".join([f"{col[0]} {col[1]}" for col in table_schema])
    create_table_query += ");"

    dest_conn.run(create_table_query)

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

with DAG('copy_table_dag', default_args=default_args, schedule='@daily') as dag:
    create_table_task = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists
    )

    copy_task = PythonOperator(
        task_id='copy_table_in_chunks',
        python_callable=copy_table_in_chunks
    )

    create_table_task >> copy_task
