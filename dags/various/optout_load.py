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
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


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
    #@task(retries=5, retry_delay=timedelta(minutes=1))
    @task
    def get_opt_out_list() -> pd.DataFrame:
        try:
            hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
            opt_out_list = hook.get_pandas_df(
                "SELECT * FROM clientresults.opt_out_list;"
            )
            return opt_out_list
        except:
            raise ValueError("Error in getting opt_out_list ...retrying in 1 minute")
    get_opt_out_list()
    print(get_opt_out_list)
    @task
    def get_local_dirs():
        dirs = os.listdir()
        return dirs
    local_dirs = get_local_dirs()
    print(f"Directories: {local_dirs}")

optout_load_dag = optout_load()

if __name__ == "__main__":
    optout_load_dag.test()
