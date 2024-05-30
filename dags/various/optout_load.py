"""
This is a DAG to load new opt outs to the opt out list
"""

import os
import sys
import time
from datetime import timedelta, datetime
import typing

from airflow import XComArg
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    params={
        "quality_check_delivery": "1",
        "script_name": "OptOut_Load",
        #"working_optout": "/data/biakonzasftp/C-9/optout_load/",
        "error_file_location": "\source\Run_Errors.txt",
    },
)
def optout_load():
    import pandas as pd
    import logging
    connection = BaseHook.get_connection('prd-az1-sqlw2-airflowconnection')
    hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
    #@task(retries=5, retry_delay=timedelta(minutes=1))
    @task
    def get_opt_out_list() -> pd.DataFrame:
        logging.info(print('/source-biakonzasftp/'))
        sourceDir = '/source-biakonzasftp/C-9/optout_load/'
        optOutFiles = os.listdir(sourceDir)
        logging.info(print(optOutFiles))
        for f in optOutFiles:
            #logging.info(print(f))
            if f == 'OptOutList.csv':
                logging.info(print('Skipping - OptOutList.csv'))
                continue
            optoutDF = pd.read_csv(sourceDir + f)
            print(f"Loading new list from: {f}")
            logging.info(print(optoutDF))
            rows = [tuple(row) for row in optoutDF.to_numpy()]
            print('Rows:')
            logging.info(print(rows))
            hook.insert_rows(table='opt_out_list_airflow_load', rows=rows, target_fields=['fname'
                ,'lname'
                ,'dob'
                ,'sex'
                ,'SSN'
                ,'respective_vault'
                ,'respective_mrn'
                ,'opt_choice'
                ,'status'
                ,'username'
                ,'user'
                ,'submitted'
                ,'completed'], replace=True)
            break
        #logging.info(print('/source-hqintellectstorage/'))
        #logging.info(os.listdir('/source-hqintellectstorage/'))
        #logging.info(print('/source-reportwriterstorage/'))
        #logging.info(os.listdir('/source-reportwriterstorage/'))
        #logging.info(print("ALL"))
        #logging.info(os.listdir('.'))
        try:
            dfOptOutList = hook.get_pandas_df(
                "SELECT * FROM clientresults.opt_out_list;"
            )
            return dfOptOutList
        except:
            raise ValueError("Error in getting opt_out_list ...retrying in 1 minute")
       
    dfOptOuts = get_opt_out_list()
    print(dfOptOuts)
    #@task
    #def get_local_dirs() -> pd.DataFrame:
    #    
    #    return dirs
    ##local_dirs = get_local_dirs()
    ##print(f"Directories: {local_dirs}")
    #get_local_dirs()
    #print("Directories: ")
    #print(get_local_dirs)

optout_load_dag = optout_load()

if __name__ == "__main__":
    optout_load_dag.test()
