from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
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
        #to='RapidAlerts_PM_C-181@konza.org;ethompson@konza.org',
        to='ethompson@konza.org',
        subject=f'Task Failed in DAG: {dag_name}',
        html_content=f"Task {context['task_instance_key_str']} failed in DAG: {dag_name}. DAG source file: {dag_file_path}. Check the logs for more details."
    )

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 17),
    'on_failure_callback': failure_callback,
}

dag = DAG(
    'RapidAlerts_Automated_Notification',
    default_args=default_args,
    description='This DAG retrieves the syslog file from a VM where it is collected and stores it for future audits with HITRUST implications',
    schedule_interval='@daily',
    catchup=False,
    tags=['C-181'],
)

@task(dag=dag)
def crawler_reference_alert(**kwargs):
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
    query = "SELECT (md5(client_reference_folder)) as ConnectionID_md5 FROM _dashboard_maintenance.crawler_reference_table where client_reference_folder IS NOT NULL;"
    dfCrawlerAudit = sql_hook.get_pandas_df(query)
    
    for index, row in dfCrawlerAudit.iterrows():
        connection_id_md5 = row['ConnectionID_md5']
        logging.info(f'Processing connection ID: {connection_id_md5}')
        
        # Assuming you have a way to map md5 to actual SFTP connection IDs
        sftp_conn_id = map_md5_to_sftp_conn_id(connection_id_md5)
        
        sftp_hook = SFTPHook(sftp_conn_id=sftp_conn_id)
        with sftp_hook.get_conn() as sftp_client:
            # Check for CSV files in the SFTP directory
            files = sftp_client.listdir()
            csv_files = [file for file in files if file.endswith('.csv')]
            logging.info(f'CSV files found: {csv_files}')
            # Additional processing can be done here

crawler_alert = crawler_reference_alert()

crawler_alert

if __name__ == "__main__":
    dag.cli()