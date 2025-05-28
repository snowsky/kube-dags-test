from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.decorators import task
from airflow.hooks.base_hook import BaseHook
import logging
import paramiko
import hashlib

dag = DAG(
    dag_id="csga_panel_auto_approval",
    start_date=datetime(2024,11,6),
    schedule_interval='@hourly',
    tags=['csga_approval','sla_52'],
    catchup=False,
) 
# Variable to store the DAG name
dag_name_base = dag.dag_id
dag_file_path_base = __file__

@task(dag=dag)
def csga_panel_auto_approval_condition_check():    
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")  # Replace with your connection ID
    sql_hook_old = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")  # Replace with your connection ID
    
    # Check against database entry in production W3 CSGA
    query = "SELECT (md5(folder_name)) as ConnectionID_md5, folder_name FROM _dashboard_requests.clients_to_process where production_auto_approval = 1;"  # Replace with your SQL query
    dfPanelAutoApproved = sql_hook.get_pandas_df(query)
   
    for index, row in dfPanelAutoApproved.iterrows():
        connection_id_md5 = row['ConnectionID_md5']
        client_reference_folder = row['folder_name']
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
            file_max_modified_time = None
            with sftp_hook.get_conn() as sftp_client:
                sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
                files = sftp_client.listdir_attr()
                csv_files = [file for file in files if file.filename.endswith('.csv')]
                logging.info(f'CSV files found: {csv_files}')
                
                for file in csv_files:
                    modified_time = pd.to_datetime(file.st_mtime, unit='s')
                    logging.info(f'File: {file.filename}, Modified Date: {modified_time}')
                    # Track the max modified time
                    if max_modified_time is None or modified_time > max_modified_time:
                        file_max_modified_time = modified_time
                if file_max_modified_time:
                    logging.info(f'Max Modified Date: {file_max_modified_time}')
                    
        
        except Exception as e:
            logging.error(f'Error Occurred: {e}')
            
        # Check against database entry in production W3 CSGA
        db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings  WHERE md5(Client) = '{connection_id_md5}' LIMIT 1"
        dfCurrentCSG = sql_hook.get_pandas_df(db_query)
        if dfCurrentCSG.empty: #Use the old DB W2 if needed
            db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings  WHERE md5(Client) = '{connection_id_md5}' LIMIT 1"
            dfCurrentCSG = sql_hook_old.get_pandas_df(db_query)
        csg_modified_time = dfCurrentCSG['event_timestamp'].iloc[0]
        csg_modified_time_offset = csg_modified_time - timedelta(days=5)

        latest_time = file_max_modified_time.strftime('%Y-%m-%d %H:%M:%S')
        if csg_modified_time_offset > latest_time:
            print("new panel to process - setting to approved")
            logging.info("new panel to process - setting to approved")
            return {
                "should_approve": True,
                "folder_name": folder_name
            }
        else:
            return {"should_approve": False}
@task
def auto_approval_update_ctp_task(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
    mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ctp',
                    mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection',
                    sql=f"""
                    UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
    )
    mysql_op.execute(dict())
@task
def auto_approval_update_ch_task(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{folder_name}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
@task
def auto_approval_update_ctp_panel_task(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{folder_name}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
    
@task
def auto_approval_update_ctp_task_old(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
@task
def auto_approval_update_ch_task_old(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `clientresults`.`client_hierarchy` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
@task
def auto_approval_update_ctp_panel_task_old(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{folder_name}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)

condition_result = csga_panel_auto_approval_condition_check()
auto_approval_update_ctp_task(condition_result)
auto_approval_update_ch_task(condition_result)
auto_approval_update_ctp_panel_task(condition_result)
#auto_approval_update_ctp_task_old(condition_result)
#auto_approval_update_ch_task_old(condition_result)
#auto_approval_update_ctp_panel_task_old(condition_result)


