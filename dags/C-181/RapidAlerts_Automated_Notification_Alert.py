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
import time

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
    description='This DAG Sends emails to a group tracking the Rapid Alerts clients to indicate new panels are available on the corresponding SFTPs',
    schedule_interval='@daily',
    catchup=False,
    tags=['C-181'],
)
# Variable to store the DAG name
dag_name_base = dag.dag_id
dag_file_path_base = __file__

@task(dag=dag)
def crawler_reference_alert(**kwargs):
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
    query = "SELECT (md5(client_reference_folder)) as ConnectionID_md5, client_reference_folder FROM _dashboard_maintenance.crawler_reference_table where client_reference_folder IS NOT NULL;"
    dfCrawlerAudit = sql_hook.get_pandas_df(query)
    
    for index, row in dfCrawlerAudit.iterrows():
        connection_id_md5 = row['ConnectionID_md5']
        client_reference_folder = row['client_reference_folder']
        logging.info(f'Processing connection ID: {connection_id_md5} for Client Folder Reference {client_reference_folder}')

        try:
            sftp_hook = SFTPHook(ssh_conn_id=connection_id_md5)
        except Exception as e:
            logging.warning(f'Failed to use ssh_conn_id: {e}')
            try:
                sftp_hook = SFTPHook(sftp_conn_id=connection_id_md5)
            except Exception as e:
                logging.error(f'Failed to use sftp_conn_id: {e}')
                continue

        try:
            with sftp_hook.get_conn() as sftp_client:
                sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
                files = sftp_client.listdir_attr()
                csv_files = [file for file in files if file.filename.endswith('.csv')]
                logging.info(f'CSV files found: {csv_files}')
                
                for file in csv_files:
                    modified_time = pd.to_datetime(file.st_mtime, unit='s')
                    logging.info(f'File: {file.filename}, Modified Date: {modified_time}')
                    
                    # Check against database entry
                    db_query = f"SELECT MAX(modified_date) as modified_date FROM clientresults.file_modification_table WHERE filename = '{file.filename}' and client_id_md5 = '{connection_id_md5}'"
                    logging.info(f'Query: {db_query}')
                    dfFileMod = sql_hook.get_pandas_df(db_query)
                    max_df_file_mod = str(dfFileMod['modified_date'][0])
                    logging.info(f'Checking if file with modified time: {modified_time} seemed greater than the DB modified time: {max_df_file_mod}')
                    if dfFileMod.empty:
                        logging.info(f'dfFileMod is Empty - emailing as if a new file was received')
                        send_email_alert(file.filename, modified_time,client_reference_folder)
                        
                        # Update the database with the new modified date
                        update_query = f"INSERT INTO clientresults.file_modification_table (filename, modified_date,client_id_md5) VALUES ('{file.filename}', '{modified_time}', '{connection_id_md5}') ON DUPLICATE KEY UPDATE modified_date = VALUES(modified_date), client_id_md5 = VALUES(client_id_md5) "
                        logging.info(f'Query: {update_query}')
                        sql_hook.run(update_query)
                    if dfFileMod['modified_date'][0] is None:
                        logging.info(f'Logic Returned False on modified date being available at zero index: {max_df_file_mod}  - emailing as if a new file was received')
                        send_email_alert(file.filename, modified_time,client_reference_folder)
                        
                        # Update the database with the new modified date
                        update_query = f"INSERT INTO clientresults.file_modification_table (filename, modified_date,client_id_md5) VALUES ('{file.filename}', '{modified_time}', '{connection_id_md5}') ON DUPLICATE KEY UPDATE modified_date = VALUES(modified_date), client_id_md5 = VALUES(client_id_md5) "
                        logging.info(f'Query: {update_query}')
                        sql_hook.run(update_query)
                    if modified_time > dfFileMod['modified_date'][0]:
                        logging.info(f'File with modified time: {modified_time} seemed greater than the DB modified time: {max_df_file_mod}')
                        send_email_alert(file.filename, modified_time,client_reference_folder)
                        
                        # Update the database with the new modified date
                        update_query = f"INSERT INTO clientresults.file_modification_table (filename, modified_date,client_id_md5) VALUES ('{file.filename}', '{modified_time}', '{connection_id_md5}') ON DUPLICATE KEY UPDATE modified_date = VALUES(modified_date), client_id_md5 = VALUES(client_id_md5) "
                        logging.info(f'Query: {update_query}')
                        sql_hook.run(update_query)
        except Exception as e:
            logging.error(f'Error Occurred: {e}')
def send_email_alert(filename, modified_time,client_id):
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
    # Check against database entry
    db_query = f"SELECT id as Client_Numeric_ID FROM _dashboard_requests.clients_to_process WHERE folder_name = '{client_id}'"
    logging.info(f'Query: {db_query}')
    dfClientQuery = sql_hook.get_pandas_df(db_query)
    client_numeric_id = str(dfClientQuery['Client_Numeric_ID'][0])
    client_distribution_list_notifier = f'RapidAlerts_PM_C-181_{client_numeric_id}@konza.org'
    logging.info(f'ID Reference Found: {client_numeric_id} on Folder Name: {client_id} and email would be sent to Distribution group: {client_distribution_list_notifier}')
    send_email(
        ##External
        #to=f'{client_distribution_list_notifier};ethompson@konza.org',
        ##Internal Testing - RapidAlerts_PM_C-181@konza.org (as of 3/26/2025)
        to='ethompson@konza.org',
        #to='RapidAlerts_PM_C-181@konza.org;ethompson@konza.org',
        #to='ethompson@konza.org;tlamond@konza.org;slewis@konza.org;cclark@konza.org',
        subject=f'KONZA has received a new file to the SFTP for Client ID {client_id} (C-181)',
        #html_content=f"Newly Modified or New CSV File: {filename} - Client Identifier/Folder Name:  {client_id} - Reporting DAG: {dag_name_base}. DAG source file: {dag_file_path_base}. Check the logs for more details."
        html_content=f"""
     <html>
     <body>
     <table border="1">
     <tr>
     <th>Newly Modified or New CSV File</th>
     <td>{filename}</td>
     </tr>
     <tr>
     <th>Client Identifier/Folder Name</th>
     <td>{client_id}</td>
     </tr>
     <tr>
     <th>Reporting DAG</th>
     <td>{dag_name_base}</td>
     </tr>
     <tr>
     <th>DAG source file</th>
     <td>{dag_file_path_base}</td>
     </tr>
     </table>
     <p>Check the logs for more details.</p>
     </body>
     </html>
     """
    )



    )
    logging.info(f'Sleeping 90 seconds per email - May need to raise rate limiter prior to removing this limiter')
    time.sleep(90)

crawler_alert = crawler_reference_alert()

crawler_alert

if __name__ == "__main__":
    dag.cli()
