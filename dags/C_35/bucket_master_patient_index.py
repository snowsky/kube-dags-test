from airflow.hooks.base import BaseHook
import trino
import mysql.connector
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

def write_to_mysql():
    # Retrieve the Trino connection details
    trino_conn = BaseHook.get_connection('trinokonza')
    trino_host = trino_conn.host
    trino_port = trino_conn.port
    trino_user = trino_conn.login
    trino_schema = trino_conn.schema

    # Connect to Trino
    trino_conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=trino_user,
        catalog='hive',
        schema=trino_schema,
    )
    trino_cursor = trino_conn.cursor()

    # Retrieve the MySQL connection details
    mysql_conn = BaseHook.get_connection('prd-az1-sqlw3-mysql-airflowconnection')
    mysql_host = mysql_conn.host
    mysql_port = mysql_conn.port
    mysql_user = mysql_conn.login
    mysql_password = mysql_conn.password
    mysql_database = mysql_conn.schema

    # Connect to MySQL
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    mysql_cursor = mysql_conn.cursor()
    # Drop if exists the table in MySQL
    mysql_cursor.execute("""
    DROP TABLE IF EXISTS thirtysix_month_lookback_airflow""")
    # Create the table in MySQL
    mysql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS thirtysix_month_lookback_airflow (
        admitted VARCHAR(255),
        source VARCHAR(255),
        unit_id VARCHAR(255),
        related_provider_id VARCHAR(255),
        accid VARCHAR(255),
        partition_month VARCHAR(255)
    )
    """)

    # Insert data into MySQL in chunks
    insert_query = """
    INSERT INTO thirtysix_month_lookback_airflow (admitted, source, unit_id, related_provider_id, accid, partition_month)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    chunk_size = 1000
    offset = 0

    while True:
        trino_cursor.execute(f"""
        SELECT admitted, source, unit_id, related_provider_id, accid, partition_month
        FROM (
            SELECT admitted, source, unit_id, related_provider_id, accid, partition_month,
                   ROW_NUMBER() OVER () AS row_num
            FROM thirtysix_month_lookback
        )
        WHERE row_num > {offset} AND row_num <= {offset + chunk_size}
        """)
        data = trino_cursor.fetchall()
        if not data:
            break
        mysql_cursor.executemany(insert_query, data)
        mysql_conn.commit()
        offset += chunk_size

    mysql_cursor.close()
    mysql_conn.close()
    trino_cursor.close()
    trino_conn.close()

with DAG(
    dag_id='thirty_six_month_lookback',
    schedule_interval='@once',
    tags=['C-35'],
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    test_connection_task = PythonOperator(
        task_id='test_trino_connection',
        python_callable=test_trino_connection,
    )

    execute_queries_task = PythonOperator(
        task_id='execute_trino_queries',
        python_callable=execute_trino_queries,
    )

    write_to_mysql_task = PythonOperator(
        task_id='write_to_mysql',
        python_callable=write_to_mysql,
    )

    test_connection_task >> execute_queries_task >> write_to_mysql_task
