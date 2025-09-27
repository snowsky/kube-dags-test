"""
# Opt Out List processing DAG
This DAG is used to determine the Master Patient Index (MPI) IDs to patients listed in a set of opt-out lists.
The opt-out lists are assumed to be stored in a POSIX-compatible local path, parameterized by the 
`OPTOUT_FILES_DIRECTORY` constant. The DAG produces an opt-out table comprising all matches of opt-out records
against the MPI table, using one of two methods:
- "standard", or a full match by firstname, lastname, dob and sex
- "special1", or a match using:
  - the last 4 digits of the SSN,
  - the first 3 characters of the lastname,
  - date of birth
  - gender
The DAG can be tested against synthetic MPI data (only using docker-compose!) by running the `populate_test_data` DAG first.
"""
import sys
sys.path.insert(0, '/opt/airflow/dags/repo/dags')

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
import datetime
from datetime import datetime
from datetime import date
from datetime import timedelta
from populations.common import CONNECTION_NAME, EXAMPLE_DATA_PATH
from populations.utils import _fix_engine_if_invalid_params

import logging
import os
from pathlib import Path

from populations.utils import _fix_engine_if_invalid_params
import logging
import os

# Change these to process real data

OPT_OUT_SCHEMA_NAME = 'clientresults'

OPT_OUT_LOAD_TABLE_NAME = 'processing_opt_out_list_airflow_load'
OPT_OUT_SANITIZED_TABLE_NAME = 'processing_opt_out_list_airflow_sanitized'
OPT_OUT_WITH_MPI_STD_TABLE_NAME = 'processing_opt_out_all_related_mpi_by_std'
OPT_OUT_WITH_MPI_SPECIAL1_TABLE_NAME = 'processing_opt_out_all_related_mpi_by_special1'
OPT_OUT_LIST_RAW_TABLE_NAME = 'processing_opt_out_list_raw'
OPT_OUT_LIST_TABLE_NAME = 'processing_opt_out_list'

INDEXED_MPI_TABLE_NAME = 'processing_opt_out_mpi_indexed'

MPI_SCHEMA_NAME = 'person_master'
MPI_TABLE_NAME = '_mpi_id_master'
OPTOUT_FILES_DIRECTORY = '/data/biakonzasftp/C-9/optout_load/'
#MYSQL_CONNECTION_ID = "qa-az1-sqlw2-airflowconnection"
MYSQL_CONNECTION_ID = "prd-az1-sqlw2-airflowconnection"
WORKING_DIR = "/data/biakonzasftp/C-9/optout_load/"
SOURCE_FILE_NAME = "global_opt_out_report_2024-06-21 thru 06-28.csv"

default_args = {
    'owner': 'airflow',
    'mysql_conn_id': 'prd-az1-sqlw2-airflowconnection', 
}

with DAG(
    'optout_processing',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions'],
) as dag:
    dag.doc_md = __doc__
    from airflow import DAG
    @task
    def add_mpis_to_database_for_patients_without_mpis(
        working_dir: str,
        source_file_name: str,
        mysql_conn_id: str,
        target_file_name: str = f"{Path(SOURCE_FILE_NAME).stem}_processed.csv",
    ):

        from pathlib import Path
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd
        from sqlalchemy.orm import sessionmaker
        import logging
        import os

        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())
        session=sessionmaker(bind= engine)()
        cursorInstance=session.connection().connection.cursor()
        #cursorInstance = engine.cursor()    
        
        source_file_path = os.path.join(working_dir, source_file_name)
        target_file_path = os.path.join(working_dir, target_file_name)

        df = pd.read_csv(source_file_path) 
        
        for i, row in df.iterrows():
            
            firstname = str(row['First Name']).replace("'", r"\'")  
            lastname = str(row['Last Name']).replace("'", r"\'")
            dob = datetime.strptime(str(row['DOB']), "%m/%d/%Y").strftime("%Y%m%d")  # '19960801'
            gender = str(row['Sex'])  # 'F'
            #logging.info(print(dob))
            #assert False
            # TODO: rewrite this as a sqlalchemy
            sqlQuery = "SELECT person_master.fn_getmpi('" + lastname + "','" + firstname + "','" + dob + "','" + gender + "');"
            
            cursorInstance.execute(sqlQuery)
            results = cursorInstance.fetchall()
            logging.info(print(f"MPI: {results}"))
            #assert False
            #mpi_val = results[0].split('(')[1].split(')')[0][0:-1]
            logging.info(print(f"{str(results[0])}"))
            #logging.info(print(f"{results[0]}"))
            #logging.info(print(f"{results[0]}"))
            #logging.info(print(f"{results[0]}"))
            
            mpi_val = str(results[0]).split('(')[1].split(')')[0][0:-1]
            mpi_val = '' if mpi_val == '0' else mpi_val

            df.at[i, 'mpi'] = mpi_val

        cursorInstance.close()    
        session.close()
        
        df.to_csv(target_file_path, index=False)

        return target_file_path

    
    processed_patients_with_new_mpis_path_in_blob_storage = add_mpis_to_database_for_patients_without_mpis(
        working_dir=WORKING_DIR,
        source_file_name=SOURCE_FILE_NAME,
        mysql_conn_id=MYSQL_CONNECTION_ID,
    )

    @task
    def load_csv_files_to_mysql(
        source_dir: str,
        target_schema: str,
        target_table: str,
        mysql_conn_id: str,
    ):

        from pathlib import Path
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd
        import logging
        from populations.common import CSV_HEADER

        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())
        #assert False, (source_dir,list(Path(source_dir).glob("*")))
        for file_path in Path(source_dir).glob("*"):
            logging.error(f"Search Started for {file_path}")
            if file_path.is_file() and '_processed' in file_path.name and file_path.name not in {'OptOutList.csv', 'processed'}:
               logging.info(f"Processing {file_path}")
               dt = pd.read_csv(file_path) 
               mask = dt.applymap(lambda x: x is None)
               cols = dt.columns[(mask).any()]
               for col in dt[cols]:
                   dt.loc[mask[col], col] = ''
               dt.rename(columns=CSV_HEADER, inplace=True)
               optoutDF = dt.astype(str)
               #assert False, list(dt.keys())
               optoutDF.to_sql(
                  name=target_table,
                  con=engine, 
                  schema=target_schema,
                  # the table already has a PRIMARY KEY set, so an index is not necessary
                  index=False,
                  # we will append records if the table already exists.
                  # Note: if multiple records with the same (fname, lname, dob, sex, ssn) combination are introduced
                  # the query will fail. This is expected. See PRIMARY KEY on opt-out table to relax this restriction.
                  if_exists='append', 
               )
    create_opt_out_schema = SQLExecuteQueryOperator(
        task_id="create_opt_out_schema",
        conn_id=MYSQL_CONNECTION_ID,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {OPT_OUT_SCHEMA_NAME};
        """
    )
    drop_opt_out_load_table_if_exists = SQLExecuteQueryOperator(
        task_id="drop_opt_out_load_table_if_exists",
        conn_id=MYSQL_CONNECTION_ID,
        sql=f"""
        DROP TABLE IF EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LOAD_TABLE_NAME}
        """
    )
    create_opt_out_load_table = SQLExecuteQueryOperator(
        task_id="create_opt_out_load_table",
        conn_id=MYSQL_CONNECTION_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {OPT_OUT_SCHEMA_NAME}.{OPT_OUT_LOAD_TABLE_NAME} (
            id VARCHAR(255),
            fname VARCHAR(255),
            lname VARCHAR(255),
            dob VARCHAR(16),
            sex VARCHAR(16),
            ssn VARCHAR(16),
            respective_vault VARCHAR(255),
            respective_mrn VARCHAR(255),
            opt_choice VARCHAR(255),
            status VARCHAR(255),
            username VARCHAR(255),
            user VARCHAR(255),
            submitted VARCHAR(255),
            completed VARCHAR(255),
            MPI VARCHAR(2555),
            last_update_php VARCHAR(255)
        );
        """,
    )    #, PRIMARY KEY (fname, lname, dob, sex, ssn) - removed from above statement

    populate_opt_out_load_table = load_csv_files_to_mysql(
        source_dir=OPTOUT_FILES_DIRECTORY,
        target_schema=OPT_OUT_SCHEMA_NAME,
        target_table=OPT_OUT_LOAD_TABLE_NAME,
    )
    processed_patients_with_new_mpis_path_in_blob_storage >> create_opt_out_schema >> drop_opt_out_load_table_if_exists >> create_opt_out_load_table >> populate_opt_out_load_table # Left after Right