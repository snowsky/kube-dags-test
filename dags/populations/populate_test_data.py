from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

from populations.common import CONNECTION_NAME, EXAMPLE_DATA_PATH

import logging
import os

SYNTHETIC_MPI_SOURCE_URL = 'https://mitre.box.com/shared/static/aw9po06ypfb9hrau4jamtvtz0e5ziucz.zip'

from test.constants import (
  SYNTHETIC_MPI_TABLE_SCHEMA,
  SYNTHETIC_MPI_TABLE_NAME,
  SYNTHETIC_MPI_CONNECTION_ID,
  TEST_OPTOUT_FILES_DIRECTORY,
)
default_args = {
    'owner': 'airflow',
}


with DAG(
    'populate_test_data',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions-dev'],
) as dag:

    @dag.task
    def create_user_defined_fn(schema_name='person_master'):
        from sqlalchemy import create_engine, text
        engine = create_engine(
            f'mysql://root:example@127.0.0.1/{schema_name}'
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
        from sqlalchemy import create_engine
        import logging

        logging.info(f'Creating table: {table_name}, in schema: {schema_name}')

        engine = create_engine(
            f'mysql://root:example@127.0.0.1/{schema_name}'
        )

        df = pd.read_csv(csv_local_file_path)
        df.to_sql(
            name=table_name,
            schema=schema_name,
            con=engine,
            if_exists='append' if append else 'replace'
        )

    def get_patients_dataframe_from_synthea_url(
        source_url: str,
        patients_csv_file_name='csv/patients.csv',
    ):
        import urllib.request, io, zipfile
        import pandas as pd

        try:
            remotezip = urllib.request.urlopen(source_url)
            zipinmemory = io.BytesIO(remotezip.read())
            zipfile = zipfile.ZipFile(zipinmemory)
            patients_file = zipfile.open(patients_csv_file_name)
            return pd.read_csv(patients_file, engine='c')
        except urllib.request.HTTPError:
            raise ValueError(f"Problem reading from {source_url}.")
    
    
    @task
    def generate_randomized_opt_out_list_from_synthetic_data(
        source_url: str,
        patients_csv_file_name='csv/patients.csv',
        opt_out_files_directory=TEST_OPTOUT_FILES_DIRECTORY,
        firstname_column="FIRST",
        lastname_column="LAST",
        gender_column="GENDER",
        dob_column="BIRTHDATE",
        ssn_column="SSN",
        mpi_table_schema=SYNTHETIC_MPI_TABLE_SCHEMA,
        mpi_table_name=SYNTHETIC_MPI_TABLE_NAME,
        rng_seed=1234,
        opt_out_file_num_rows=100,
    ):
        import pandas as pd
        import random
        from sqlalchemy import create_engine, text
        from pathlib import Path
        import os
        from populations.common import CSV_HEADER
        
        colnames = {v: k for k, v in CSV_HEADER.items()}

        Path(opt_out_files_directory).mkdir(parents=True, exist_ok=True)

        dt = get_patients_dataframe_from_synthea_url(source_url, patients_csv_file_name)

        logging.info(dt.keys())
        random.seed(rng_seed)
        opt_choices = random.choices(['in', 'not in'], k=dt.shape[0])
        dt = pd.DataFrame({
            colnames['fname']: dt[firstname_column],
            colnames['lname']: dt[lastname_column],
            colnames['dob']: dt[dob_column],
            colnames['sex']: dt[gender_column],
            colnames['ssn']: dt[ssn_column],
            colnames['respective_vault']: 'TBD',
            colnames['respective_mrn']: 'TBD',
            colnames['opt_choice']: opt_choices,
            colnames['status']: 'TBD',
            colnames['username']: 'TBD',
            colnames['user']: 'TBD',
            colnames['submitted']: 'TBD',
            colnames['completed']: 'TBD',
        })
        for start in range(0, dt.shape[0], opt_out_file_num_rows):
            end = start + opt_out_file_num_rows
            if end > dt.shape[0]:
                end = dt.shape[0]

            subset = dt.iloc[start:end,:]
            subset_path = os.path.join(opt_out_files_directory, f'test_synthea_opt_out_{start}.csv')
            subset.to_csv(subset_path, index=False)

    @task
    def write_synthetic_data_to_mpi_table(
        source_url,
        target_connection_id,
        patients_csv_file_name='csv/patients.csv',
        firstname_column="FIRST",
        lastname_column="LAST",
        gender_column="GENDER",
        dob_column="BIRTHDATE",
        ssn_column="SSN",
        mpi_table_schema=SYNTHETIC_MPI_TABLE_SCHEMA,
        mpi_table_name=SYNTHETIC_MPI_TABLE_NAME,

    ):
        import pandas as pd
        from sqlalchemy import create_engine, text
        
        dt = get_patients_dataframe_from_synthea_url(source_url, patients_csv_file_name)

        dt = pd.DataFrame({
            'id': range(dt.shape[0]),
            'firstname': dt[firstname_column],
            'lastname': dt[lastname_column],
            'dob': dt[dob_column],
            'sex': dt[gender_column],
            'ssn': dt[ssn_column],
            'longitudinal_anomoly': 'TBD',
            'first_name_anomoly': 'TBD',
            'mrn_anomoly': 'TBD',

        })
        dt.set_index(['firstname', 'lastname', 'dob', 'sex'], inplace=True)
        engine = create_engine(
            f'mysql://root:example@{target_connection_id.lower()}/{mpi_table_schema}'
        )

        dt.to_sql(
            name=mpi_table_name,
            schema=mpi_table_schema,
            con=engine,
            if_exists='replace'
        )

    mpi_schema = SQLExecuteQueryOperator(
        task_id=f'create_mpi_schema',
        conn_id=SYNTHETIC_MPI_CONNECTION_ID,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SYNTHETIC_MPI_TABLE_SCHEMA};
        """,
        dag=dag
    )
    if 'TEST_ENV' in os.environ and os.environ["TEST_ENV"] == 'true': 
        synthetic_mpi_table = write_synthetic_data_to_mpi_table(
            source_url=SYNTHETIC_MPI_SOURCE_URL,
            target_connection_id=SYNTHETIC_MPI_CONNECTION_ID,
        )
        randomized_opt_out = generate_randomized_opt_out_list_from_synthetic_data(
            source_url=SYNTHETIC_MPI_SOURCE_URL,
        )
        mpi_schema >> synthetic_mpi_table
        
        create_schema_and_tables = upload_csvs_to_db()
        create_udf = create_user_defined_fn()

        create_schema_and_tables >> create_udf
