# SLA-113 - Removal of ACCID
# This DAG is used to erase from the Data Warehouse visits attributable to patients including those found in the storage accounts where extracts are derived and the MySQL database where the records are mapped.

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
from populations.common import CONNECTION_NAME, EXAMPLE_DATA_PATH
from populations.target_population_impl import _fix_engine_if_invalid_params
assert False
import logging
import os
from pathlib import Path

from populations.target_population_impl import _fix_engine_if_invalid_params
import logging
import os


MPI_SCHEMA_NAME = 'person_master'
MPI_TABLE_NAME = '_mpi'
SOURCE_FILES_DIRECTORY = '/source-biakonzasftp/C-9/SLA-113/'

default_args = {
    'owner': 'airflow',
    'mysql_conn_id': 'prd-az1-sqlw2-airflowconnection', 
}

with DAG(
    'sla_113__removal_of_accid',
    default_args=default_args,
    schedule=None,
    tags=['SLA-113'],
) as dag:
    dag.doc_md = __doc__

with DAG(
    'optout_processing',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions'],
) as dag:
    dag.doc_md = __doc__
    from airflow import DAG
    @task
    def load_csv_files_to_mysql(
        working_dir: str,
        target_schema: str
    ):

        from pathlib import Path
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd
        from sqlalchemy.orm import sessionmaker
        import logging
        import os

        hook = MySqlHook(conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())
        session=sessionmaker(bind= engine)()
        cursorInstance=session.connection().connection.cursor()
        #cursorInstance = engine.cursor()    
        
        source_file_path = os.path.join(working_dir, source_file_name)
        target_file_path = os.path.join(working_dir, target_file_name)
        for file_path in Path(working_dir).glob("*"):
            logging.error(f"Search Started for {file_path}")
            if file_path.is_file() and file_path.name not in {'processed'}:
                logging.info(f"Processing {file_path}")
                df = pd.read_csv(source_file_path) 
                logging.info(print(f"dataframe: {df}"))
    max_id = MySQL
    # Set task dependencies
    load_csv >> max_id

if __name__ == "__main__":
    dag.cli()
