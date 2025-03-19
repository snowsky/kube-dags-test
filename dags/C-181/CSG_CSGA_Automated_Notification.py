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
import traceback

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
    'CSG_CSGA_Automated_Notification',
    default_args=default_args,
    description='',
    schedule_interval='@daily',
    catchup=False,
    tags=['C-181'],
)
# Variable to store the DAG name
dag_name_base = dag.dag_id
dag_file_path_base = __file__

@task(dag=dag)
def csg_alert(**kwargs):
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
    sql_hook_old = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
    query = "SELECT (md5(client_reference_folder)) as client_md5, client_reference_folder FROM _dashboard_maintenance.crawler_reference_table where client_reference_folder IS NOT NULL;"
    dfCrawlerAudit = sql_hook.get_pandas_df(query)
    
    for index, row in dfCrawlerAudit.iterrows():
        client_md5 = row['client_md5']
        client_reference_folder = row['client_reference_folder']
        logging.info(f'Processing connection ID: {client_md5} for Client Folder Reference {client_reference_folder}')
        try:
            # Check against database entry in production W3 CSGA
            db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings_approved  WHERE md5(Client) = '{client_md5}' LIMIT 1"
            dfCurrentCSGA = sql_hook.get_pandas_df(db_query)
            if dfCurrentCSGA.empty: #Use the old DB W2 if needed
                db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings_approved  WHERE md5(Client) = '{client_md5}' LIMIT 1"
                dfCurrentCSGA = sql_hook_old.get_pandas_df(db_query)
            # Check against database entry in production W3 CSGA
            db_query = f"select count_distinct_mpi, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings_approved_running_counts  WHERE md5(Client) = '{client_md5}' LIMIT 1"
            dfCurrentCSGACount = sql_hook.get_pandas_df(db_query)
            if dfCurrentCSGACount.empty: #Use the old DB W2 if needed
                db_query = f"select count_distinct_mpi, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings_approved_running_counts  WHERE md5(Client) = '{client_md5}' LIMIT 1"
                dfCurrentCSGACount = sql_hook_old.get_pandas_df(db_query)
            Client = dfCurrentCSGA['Client'].iloc[0]
            CSG_or_CSGA = 'CSGA'
            modified_time = dfCurrentCSGA['event_timestamp'].iloc[0]
            md5 = dfCurrentCSGA['md5'].iloc[0]
            C60popCount = dfCurrentCSGACount['count_distinct_mpi'].iloc[0]
            C60modified_time = dfCurrentCSGACount['event_timestamp'].iloc[0]
            # Check against database entry
            db_query = f"SELECT modified_date FROM clientresults.csg_modification_table WHERE client_id_md5 = '{client_md5}'"
            dfModificationCheck = sql_hook.get_pandas_df(db_query)
            if dfModificationCheck.empty or modified_time > dfModificationCheck['modified_date'].max():
                send_email_alert(CSG_or_CSGA, modified_time,client_reference_folder,C60popCount,C60modified_time)
                # Update the database with the new modified date
                update_query = f"REPLACE INTO clientresults.csg_modification_table (Client,CSG_or_CSGA, modified_date,client_id_md5) VALUES ('{Client}','{CSG_or_CSGA}',  '{modified_time}', '{md5}')"
                sql_hook.run(update_query)
        except Exception as e:
            tb = traceback.format_exc()
            logging.error(f'Error Occurred: {e}\nTraceback:\n{tb}')



        try:
            # Check against database entry in production W3 CSGA
            db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings  WHERE md5(Client) = '{client_md5}' LIMIT 1"
            dfCurrentCSG = sql_hook.get_pandas_df(db_query)
            if dfCurrentCSG.empty: #Use the old DB W2 if needed
                db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings  WHERE md5(Client) = '{client_md5}' LIMIT 1"
                dfCurrentCSG = sql_hook_old.get_pandas_df(db_query)
            # Check against database entry in production W3 CSGA
            db_query = f"select count_distinct_mpi, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings_running_counts  WHERE md5(Client) = '{client_md5}' LIMIT 1"
            dfCurrentCSGCount = sql_hook.get_pandas_df(db_query)
            if dfCurrentCSGCount.empty: #Use the old DB W2 if needed
                db_query = f"select count_distinct_mpi, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings_running_counts  WHERE md5(Client) = '{client_md5}' LIMIT 1"
                dfCurrentCSGCount = sql_hook_old.get_pandas_df(db_query)
            Client = dfCurrentCSG['Client'].iloc[0]
            CSG_or_CSGA = 'CSG'
            modified_time = dfCurrentCSG['event_timestamp'].iloc[0]
            md5 = dfCurrentCSG['md5'].iloc[0]
            C60popCount = dfCurrentCSGCount['count_distinct_mpi'].iloc[0]
            C60modified_time = dfCurrentCSGCount['event_timestamp'].iloc[0]
            # Check against database entry
            db_query = f"SELECT modified_date FROM clientresults.csg_modification_table WHERE client_id_md5 = '{client_md5}'"
            dfModificationCheck = sql_hook.get_pandas_df(db_query)
            if dfModificationCheck.empty or modified_time > dfModificationCheck['modified_date'].max():
                send_email_alert(CSG_or_CSGA, modified_time,client_reference_folder,C60popCount,C60modified_time)
                            
                # Update the database with the new modified date
                update_query = f"REPLACE INTO clientresults.csg_modification_table (Client,CSG_or_CSGA, modified_date,client_id_md5) VALUES ('{Client}','{CSG_or_CSGA}',  '{modified_time}', '{md5}')"
                sql_hook.run(update_query)
        except Exception as e:
            tb = traceback.format_exc()
            logging.error(f'Error Occurred: {e}\nTraceback:\n{tb}')


       
def send_email_alert(CSG_or_CSGA_indicator,modified_time,client_id,c60_populationCount,c60_modified_time):
    send_email(
        #to='RapidAlerts_PM_C-181@konza.org;ethompson@konza.org',
        to='ethompson@konza.org',
        subject=f'Newly Modified CSG in Category: {CSG_or_CSGA_indicator} for Client ID {client_id} (C-181)',
        html_content=f"Newly Modified CSG in Category: {CSG_or_CSGA_indicator} - Client Identifier/Folder Name {client_id} - Based on the C-60 project adherence, here is the population count {c60_populationCount} based on the last reported count timestamp of {c60_modified_time} Reporting DAG: {dag_name_base}. If the timestamps do not match, make sure your population definition requests a C-60 table update.  DAG source file: {dag_file_path_base}. Check the logs for more details."
    )

csg_alert = csg_alert()

csg_alert

if __name__ == "__main__":
    dag.cli()
