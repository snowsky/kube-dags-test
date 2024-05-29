from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.settings import Session
from airflow.utils.dates import days_ago
from airflow.decorators import task

from populations.common import CONNECTION_NAME, EXAMPLE_SCHEMA_NAME, EXAMPLE_DATA_PATH

default_args = {
    'owner': 'airflow',
}

def upload_csv_file_to_db(
    csv_local_file_path: str,
    schema_name: str,
    table_name: str,
    connection_name=CONNECTION_NAME,
    append=False,
):
    
    import pandas as pd
    from sqlalchemy import create_engine, types

    engine = create_engine(
        f'mysql://root:example@mariadb/{schema_name}'
    ) 

    df = pd.read_csv(csv_local_file_path)
    df.to_sql(
        name=table_name, 
        schema=schema_name, 
        con=engine, 
        if_exists='append' if append else 'replace'
    )

@task
def upload_csvs_to_db(schema_name=EXAMPLE_SCHEMA_NAME):
    
    from pathlib import Path
    import os

    tables = []
    for file_name in os.listdir(EXAMPLE_DATA_PATH):
        table_name = Path(file_name).stem
        csv_file_path = os.path.join(EXAMPLE_DATA_PATH, file_name)
        upload_csv_file_to_db(csv_file_path, schema_name, table_name)
        tables.append(table_name)
    return tables

dag = DAG(
    'populate_test_data',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['example', 'population-definitions'],
)

create_schema = MySqlOperator(
    task_id='create_schema',
    mysql_conn_id=CONNECTION_NAME, 
    sql=f"""
    CREATE SCHEMA IF NOT EXISTS {EXAMPLE_SCHEMA_NAME};
    """, 
    dag=dag
)
create_tables = upload_csvs_to_db()

create_schema >> create_tables
