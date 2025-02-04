## Aishwarya here is a basic template - We would need to build it out, but the connectivity and the table reference are correct for the 2 hour alert described in SUP-10813
# Needs thorough testing
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
import os
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the failure callback function
def failure_callback(context):
    dag_name = context['dag'].dag_id
    dag_file_path = context['dag'].fileloc
    send_email(
        to='tlamond@konza.org;ddooley@konza.org;cclark@konza.org;jdenson@konza.org',
        subject=f'Task Failed in DAG: {dag_name}',
        html_content=f"Task {context['task_instance_key_str']} failed in DAG: {dag_name}. DAG source file: {dag_file_path}. Check the logs for more details."
    )
def crawler_reference_alert(**kwargs):    
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")  # Replace with your connection ID
    query = "SELECT * FROM _dashboard_maintenance.crawler_reference_table;"  # Replace with your SQL query
    dfCrawlerAudit = sql_hook.get_pandas_df(query)

