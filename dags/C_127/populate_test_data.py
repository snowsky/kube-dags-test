import sys
sys.path.insert(0, '/opt/airflow/dags/repo/dags')

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

from populations.utils import _fix_engine_if_invalid_params # Move this to common location

CONNECTION_NAME = 'MariaDB'
EXAMPLE_DATA_PATH = '/test/C-127/db_tables'

import os

default_args = {
    'owner': 'airflow',
}

with DAG(
    'populate_test_data_C-127',
    default_args=default_args,
    schedule=None,
    tags=['example', 'C-127'],
) as dag:

    @dag.task
    def upload_csvs_to_db():
        from pathlib import Path
        import os

        schema = []
        tables = []
        for file_name in os.listdir(EXAMPLE_DATA_PATH):
            if '.csv' in file_name:
                file_name_stem = Path(file_name).stem
                schema_name, table_name = file_name_stem.split('__', 1)
                if schema_name not in schema:
                    _create_schema(schema_name)
                    schema.append(schema_name)
                if table_name:
                    csv_file_path = os.path.join(EXAMPLE_DATA_PATH, file_name)
                    upload_csv_file_to_db(csv_file_path, schema_name, table_name)
                    tables.append(table_name)
        return tables


    def _create_schema(schema_name, conn_id=CONNECTION_NAME):
        import logging
        logging.info(f'Creating schema: {schema_name}')
        create_schema_op = SQLExecuteQueryOperator(
            task_id=f'create_schema_{schema_name}',
            conn_id=conn_id,
            sql=f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name};
            """,
            dag=dag
        )
        return create_schema_op.execute(dict())


    def upload_csv_file_to_db(
        csv_local_file_path: str,
        schema_name: str,
        table_name: str,
        connection_name=CONNECTION_NAME,
        append=False,
    ):

        import pandas as pd
        import logging

        logging.info(f'Creating table: {table_name}, in schema: {schema_name}')

        hook = MySqlHook(conn_id=CONNECTION_NAME)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())

        df = pd.read_csv(csv_local_file_path)
        df.to_sql(
            name=table_name,
            schema=schema_name,
            con=engine,
            if_exists='append' if append else 'replace'
        )

test_env = Variable.get("TEST_ENV")
if test_env:
    create_schema_and_tables = upload_csvs_to_db()
    create_schema_and_tables
