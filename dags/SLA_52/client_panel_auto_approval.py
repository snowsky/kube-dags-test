from airflow.decorators import dag, task
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime,timedelta
from airflow.hooks.base_hook import BaseHook
import logging
import paramiko
import hashlib


@task
def csga_panel_auto_approval_condition_check():
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")
    sql_hook_old = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
    
    query = "SELECT md5(folder_name) as ConnectionID_md5, folder_name FROM _dashboard_requests.clients_to_process WHERE production_auto_approval = 1 and frequency <> 'Approved';"
    dfPanelAutoApproved = sql_hook.get_pandas_df(query)
    if dfPanelAutoApproved.empty:
        return []

    results = []

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
            latest_file_name = None
            with sftp_hook.get_conn() as sftp_client:
                files = sftp_client.listdir_attr()
                csv_files = [file for file in files if file.filename.endswith('.csv')]
                for file in csv_files:
                    modified_time = pd.to_datetime(file.st_mtime, unit='s')
                    if file_max_modified_time is None or modified_time > file_max_modified_time:
                        file_max_modified_time = modified_time
                        latest_file_name = file.filename
        except Exception as e:
            logging.error(f'Error Occurred: {e}')
            continue

        db_query = f"SELECT Client, event_timestamp, md5(Client) as md5 FROM clientresults.client_security_groupings WHERE md5(Client) = '{connection_id_md5}' LIMIT 1"
        dfCurrentCSG = sql_hook.get_pandas_df(db_query)
        if dfCurrentCSG.empty:
            dfCurrentCSG = sql_hook_old.get_pandas_df(db_query)
        if dfCurrentCSG.empty:
            continue

        csg_modified_time = dfCurrentCSG['event_timestamp'].iloc[0]
        csg_modified_time_offset = csg_modified_time + timedelta(hours=24)
        logging.info(f'Testing if file_max_modified_time > csg_modified_time_offset with csg_modified_time_offset: {csg_modified_time_offset} - file_max_modified_time: {file_max_modified_time}')
        if file_max_modified_time > csg_modified_time_offset:
            logging.info(f'Adding to set to Approved List')
            results.append({
                "should_approve": True,
                "folder_name": client_reference_folder,
                "latestfile": latest_file_name,
                "latest_time": file_max_modified_time.strftime('%Y-%m-%d %H:%M:%S'),
                "md5": connection_id_md5
            })
        else:
            logging.info(f'Not Approved to Process - Condition not met')
            results.append({
                "should_approve": False,
                "folder_name": client_reference_folder,
                "md5": connection_id_md5
            })

    return results


@task
def auto_approval_update_ctp_task(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{data['folder_name']}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
    logging.info(f'Ran: {sql}')
@task
def auto_approval_update_ch_task(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{data['folder_name']}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
    logging.info(f'Ran: {sql}')
@task
def auto_approval_update_ctp_panel_task(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{data['folder_name']}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection')
    hook.run(sql)
    logging.info(f'Ran: {sql}')
    
@task
def auto_approval_update_ctp_task_old(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{data['folder_name']}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw2-airflowconnection')
    hook.run(sql)
    logging.info(f'Ran: {sql}')
@task
def auto_approval_update_ch_task_old(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `clientresults`.`client_hierarchy` SET `frequency`='Approved' WHERE `folder_name`='{data['folder_name']}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw2-airflowconnection')
    hook.run(sql)
    logging.info(f'Ran: {sql}')
@task
def auto_approval_update_ctp_panel_task_old(data: dict):
    if not data["should_approve"]:
        logging.info("Skipping update_ctp")
        return
    sql=f"""
        UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{data['folder_name']}';
    """
    hook = MySqlHook(mysql_conn_id='prd-az1-sqlw2-airflowconnection')
    hook.run(sql)
    logging.info(f'Ran: {sql}')

@dag(
    dag_id="csga_panel_auto_approval",
    start_date=datetime(2024,11,6),
    schedule_interval='@hourly',
    tags=['csga_approval','sla_52'],
    catchup=False,
) 

def csga_panel_auto_approval_dag():
    condition_results = csga_panel_auto_approval_condition_check()
    auto_approval_update_ctp_task.expand(data=condition_results)
    auto_approval_update_ch_task.expand(data=condition_results)
    auto_approval_update_ctp_panel_task.expand(data=condition_results)
    #auto_approval_update_ctp_task_old.expand(data=condition_results)
    #auto_approval_update_ch_task_old.expand(data=condition_results)
    #auto_approval_update_ctp_panel_task_old.expand(data=condition_results)

csga_panel_auto_approval_dag()
