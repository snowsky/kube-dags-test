from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

from populations.common import CONNECTION_NAME, EXAMPLE_DATA_PATH

default_args = {
    'owner': 'airflow',
}


with DAG(
    'populate_test_data',
    default_args=default_args,
    start_date=days_ago(2),
    tags=['example', 'population-definitions'],
) as dag:

    @dag.task
    def create_user_defined_fn(schema_name='person_master'):
        from sqlalchemy import create_engine, text
        engine = create_engine(
            f'mysql://root:example@{CONNECTION_NAME.lower()}/{schema_name}'
        )

        create_udf_sql = """
        DROP FUNCTION IF EXISTS fn_getmpi;
        CREATE FUNCTION fn_getmpi(a VARCHAR(255), b VARCHAR(255), c VARCHAR(255), d VARCHAR(255))
        RETURNS INT
        DETERMINISTIC
        RETURN 1337;
        """
        with engine.connect() as connection:
            connection.execute(text(create_udf_sql))


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
        create_schema_op = MySqlOperator(
            task_id=f'create_schema_{schema_name}',
            mysql_conn_id=conn_id,
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
        from sqlalchemy import create_engine
        import logging

        logging.info(f'Creating table: {table_name}, in schema: {schema_name}')

        engine = create_engine(
            f'mysql://root:example@{connection_name.lower()}/{schema_name}'
        )

        df = pd.read_csv(csv_local_file_path)
        df.to_sql(
            name=table_name,
            schema=schema_name,
            con=engine,
            if_exists='append' if append else 'replace'
        )


create_schema_and_tables = upload_csvs_to_db()
create_udf = create_user_defined_fn()

create_schema_and_tables >> create_udf
