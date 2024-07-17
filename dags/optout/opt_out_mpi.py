from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago

from populations.common import CONNECTION_NAME, EXAMPLE_DATA_PATH
from populations.target_population_impl import _fix_engine_if_invalid_params

import logging
import os
from pathlib import Path

MYSQL_CONNECTION_ID = "TBD"
WORKING_DIR = "TBD"
SOURCE_FILE_NAME = "TBD"

default_args = {
    'owner': 'airflow',
    'mysql_conn_id': MYSQL_CONNECTION_ID
}
with DAG(
    'process_opt_out_list',
    default_args=default_args,
    schedule=None,
    tags=['example', 'population-definitions'],
) as dag:

    @task
    def add_mpis_to_database_for_patients_without_mpis(
        working_dir: str,
        source_file_name: str,
        target_schema: str,
        target_table: str,
        mysql_conn_id: str,
        target_file_name: str = f"{Path(source_file_name).stem}_processed.csv",
    ):

        from pathlib import Path
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import pandas as pd
        import logging
        import os

        hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        engine = _fix_engine_if_invalid_params(hook.get_sqlalchemy_engine())
        
        cursorInstance = engine.cursor()    
        
        source_file_path = os.path.join(working_dir, source_file_name)
        target_file_path = os.path.join(working_dir, target_file_name)

        df = pd.read_csv(source_file_path) 
        
        for i, row in df.iterrows():
            
            firstname = str(row['FRST_NM']).replace("'", r"\'")  
            lastname = str(row['LAST_NM']).replace("'", r"\'")
            dob = datetime.strptime(str(row['BRTH_DT']), "%m/%d/%Y").strftime("%Y%m%d")  # '19960801'
            gender = str(row['GNDR_CD'])  # 'F'

            # TODO: rewrite this as a sqlalchemy
            sqlQuery = "SELECT person_master.fn_getmpi('" + lastname + "','" + firstname + "','" + dob + "','" + gender + "');"
            
            cursorInstance.execute(sqlQuery)
            results = cursorInstance.fetchall()

            mpi_val = str(results[-1]).split(":")[1][1:-1]
            mpi_val = '' if mpi_val == '0' else mpi_val

            df.at[i, 'mpi'] = mpi_val

        cursorInstance.close()    
        mySQLConnection.close()
        
        df.to_csv(target_file_path)

        return target_file_path

    
    processed_patients_with_new_mpis_path_in_blob_storage = add_mpis_to_database_for_patients_without_mpis(
        working_dir=WORKING_DIR,
        source_file_name=SOURCE_FILE_NAME,
        mysql_conn_id=MYSQL_CONN_ID,
    )
