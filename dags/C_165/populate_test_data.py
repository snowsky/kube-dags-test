from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import logging
import os

CONNECTION_NAME = "MariaDB"
EXAMPLE_DATA_PATH = "/opt/airflow/example_data"

default_args = {
    "owner": "airflow",
}

with DAG(
    "populate_test_data_c165",
    default_args=default_args,
    schedule=None,
    tags=["test", "C-165"],
) as dag:

    @dag.task
    def upload_csvs_to_db():

        from pathlib import Path
        import os

        schema = []
        tables = []
        for file_name in os.listdir(EXAMPLE_DATA_PATH):
            if ".csv" in file_name and "~lock" not in file_name:
                file_name_stem = Path(file_name).stem
                schema_name, table_name = file_name_stem.split("__", 1)
                if schema_name not in schema:
                    _create_schema(schema_name)
                    schema.append(schema_name)
                if table_name:
                    csv_file_path = os.path.join(EXAMPLE_DATA_PATH, file_name)
                    _upload_csv_file_to_db(csv_file_path, schema_name, table_name)
                    tables.append(table_name)
        return tables

    def _create_schema(schema_name, conn_id=CONNECTION_NAME):
        import logging

        logging.info(f"Creating schema: {schema_name}")
        create_schema_op = SQLExecuteQueryOperator(
            task_id=f"create_schema_{schema_name}",
            conn_id=conn_id,
            sql=f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name};
            """,
            dag=dag,
        )
        return create_schema_op.execute(dict())

    def _upload_csv_file_to_db(
        csv_local_file_path: str,
        schema_name: str,
        table_name: str,
        connection_name=CONNECTION_NAME,
        append=False,
    ):

        import pandas as pd
        from sqlalchemy import create_engine
        import logging

        logging.info(f"Creating table: {table_name}, in schema: {schema_name}")

        engine = create_engine(f"mysql://root:example@MariaDB/{schema_name}")

        df = pd.read_csv(csv_local_file_path)
        df.to_sql(
            name=table_name,
            schema=schema_name,
            con=engine,
            if_exists="append" if append else "replace",
        )

    if "TEST_ENV" in os.environ and os.environ["TEST_ENV"] == "true":

        create_schema_and_tables = upload_csvs_to_db()

        create_schema_and_tables
