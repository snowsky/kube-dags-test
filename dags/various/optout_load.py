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
        "days_offset": 0,  # Set to -13 for example to start 13 days ago
        "error_file_location": "\source\Run_Errors.txt",
    },
)
def optout_load():
    import pandas as pd
    @task(retries=5, retry_delay=timedelta(minutes=1))
    def get_parser_check() -> pd.DataFrame:
        try:
            hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
            parser_check = hook.get_pandas_df(
                "SELECT * FROM clientresults.opt_out_list;"
            )
            return parser_check
        except:
            raise ValueError("Error in getting parser check...retrying in 1 minute")
optout_load_dag = optout_load()

if __name__ == "__main__":
    optout_load_dag.test()
