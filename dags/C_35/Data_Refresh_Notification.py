from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta, time
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
        #to='QualityTeam@konza.org;ethompson@konza.org',
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
    'Data_Refresh_Notification',
    default_args=default_args,
    description='This DAG Sends emails to a group tracking the Data Refresh for HQ Insights and updating the SLA',
    schedule_interval='@daily',
    catchup=False,
    tags=['C-35'],
)
# Variable to store the DAG name
dag_name_base = dag.dag_id
dag_file_path_base = __file__

@task(dag=dag)
def get_latest_records():
    mysql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
    mssql_hook = MsSqlHook(mssql_conn_id='formoperations_prd_az1_opssql_database_windows_net')
    engine = mssql_hook.get_sqlalchemy_engine()
    conn = mssql_hook.get_conn()
    
    # SQL query to get the latest records by max ID grouped by server_dns
    query = """
    SELECT dr.id, dr.server_dns, dr.data_update_date
    FROM dashboard_refresh dr
    JOIN (
        SELECT server_dns, MAX(id) AS max_id
        FROM dashboard_refresh
        GROUP BY server_dns
    ) subquery ON dr.server_dns = subquery.server_dns AND dr.id = subquery.max_id
    ORDER BY dr.id DESC;
    """
    
    # Load the query results into a DataFrame
    dfDataRefreshAudit = pd.read_sql(query, conn)
        
    # Cycle through the server IDs in a for loop
    for index, row in dfDataRefreshAudit.iterrows():
        server_id = row['server_dns']
        data_update_date = row['data_update_date']
        logging.info(f"Processing server: {server_id}")
        db_query = f"SELECT MAX(data_update_date) as data_update_date FROM clientresults.data_refresh_update_table WHERE server_dns = '{server_id}'"
        logging.info(f'Query: {db_query}')
        dfDataMod = mysql_hook.get_pandas_df(db_query)
        max_df_data_mod = str(dfDataMod['data_update_date'][0])
        logging.info(f'Var max_df_data_mod: {max_df_data_mod}')
        # Convert the string to a datetime object
        data_update_date_dt = datetime.combine(datetime.strptime(data_update_date, '%Y-%m-%d'), time(0, 0))
        logging.info(f'Var data_update_date_dt: {data_update_date_dt}')
        # Convert the first element of dfDataMod['data_update_date'] to a datetime object
        dfDataMod_date_dt = datetime.combine(datetime.strptime('1970-01-01', '%Y-%m-%d'), time(0, 0))
        logging.info(f'Var dfDataMod_date_dt: {dfDataMod_date_dt}')
        if dfDataMod['data_update_date'][0] is not None:
            dfDataMod_date_dt = datetime.strptime(str(dfDataMod['data_update_date'][0]), '%Y-%m-%d  %H:%M:%S')
            logging.info(f'Var dfDataMod_date_dt after if: {dfDataMod_date_dt}')

        logging.info(f'Checking if Data Update with date: {data_update_date} seemed greater than the DB date: {max_df_data_mod}')
        
        if dfDataMod.empty or data_update_date_dt > dfDataMod_date_dt:
            logging.info(f'Data Update with date: {data_update_date} seemed greater than the DB date: {max_df_data_mod}')
            # Update the database with the new update date
            update_query = f"REPLACE INTO clientresults.data_refresh_update_table (server_dns, data_update_date) VALUES ('{server_id}', '{data_update_date}')"
            mysql_hook.run(update_query)
            if 'prd-az1-an17' in server_id:
                logging.info(f"Detected SacValley on : {server_id}")
                send_email_alert(server_id, data_update_date)
            if 'prd-az1-an17' not in server_id:
                logging.info(f"Detected KONZA based server on : {server_id}")
                send_email_alert(server_id, data_update_date)

def send_email_alert(server_id, data_update_date):
    logging.info(f"Detected KONZA based server on : {server_id}")
    send_email(
                    ##External
                    #to=f'{client_distribution_list_notifier};ethompson@konza.org',
                    ##Internal Testing - 
                    #to='ethompson@konza.org',
                    to='QualityTeam@konza.org;ethompson@konza.org',
                    #to='ethompson@konza.org;mtally@konza.org;cclark@konza.org',
                    subject=f'KONZA has refresh the data on {server_id} (C-35)',
                    html_content=f"Newly Updated Dataset: {server_id} - With Update Date:  {data_update_date} - Please cross check with the Weekly Newsletter server refresh dates - Reporting DAG: {dag_name_base}. DAG source file: {dag_file_path_base}. Check the logs for more details."
                )

latest_records = get_latest_records()

latest_records

if __name__ == "__main__":
    dag.cli()
