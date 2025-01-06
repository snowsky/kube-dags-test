from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.decorators import task
from airflow.hooks.base_hook import BaseHook
import logging
import paramiko
import hashlib


def csga_panel_auto_approval_data_pull(**kwargs):    
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")  # Replace with your connection ID
    query = "SELECT * FROM _dashboard_requests.clients_to_process_panel_audit;"  # Replace with your SQL query
    dfPanelAudit = sql_hook.get_pandas_df(query)
    
    #print(dfPanelAudit)
    for index, row in dfPanelAudit.iterrows():
        dfObj = sql_hook.get_pandas_df
        folder_name = row['folder_name'].strip()
        folder_name_conn_id = hashlib.md5(folder_name.encode()).hexdigest()
        logging.info(f'md5 Hash should be {folder_name_conn_id} for client {folder_name}')
        most_recent_filename = row['most_recent_filename'].strip()
        most_recent_file_timestamp = row['most_recent_file_timestamp'].strip()

        from airflow.hooks.base_hook import BaseHook
        connection = BaseHook.get_connection(folder_name_conn_id)
        username = connection.login
        password = connection.password
        server = connection.host
        port = connection.port
        logging.info(f'username should be {username}')
        logging.info(f'password should be {password}')
        logging.info(f'server should be {server}')
        logging.info(f'port should be {port}')

        logging.info(f'folder name should be {folder_name}')
        postgres_hook = PostgresHook(postgres_conn_id="prd-az1-ops3-airflowconnection")  # Replace with your ops postgres connection ID
        query = """select * from sftp_credentials_parent_mgmt where client_security_groupings_admins_name = '""" + folder_name + """'; """  # 
        dfdbcreds = postgres_hook.get_pandas_df(query)
        print(dfdbcreds)

        dbcred_sftp_root_directory = dfdbcreds['sftp_root_directory'][0]
        logging.info(f'dbcred_sftp_root_directory should be {dbcred_sftp_root_directory}')

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=server, port=port, username=username, password=password)
        ftp_client = ssh_client.open_sftp()

        sourcePath = dbcred_sftp_root_directory

        latest = 0
        latestfile = None

        for fileattr in ftp_client.listdir_attr(sourcePath):
            #print(fileattr)
            if fileattr.filename.endswith('.csv') and fileattr.st_mtime > latest:
                latest = fileattr.st_mtime
                latestfile = fileattr.filename
                #logging.info(f'Latest file should be {latestfile}')

        ftp_client.close()
        ssh_client.close()
        print(folder_name)

        latest_time = datetime.fromtimestamp(latest).strftime('%Y-%m-%d %H:%M:%S')
        if latest_time > most_recent_file_timestamp:
            print("new panel to process - setting to approved")
            def auto_approval_update_ctp_task(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ctp_{index}',
                    mysql_conn_id='prd-az1-sqlw2-airflowconnection',
                    sql=f"""
                    UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ch_task(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ch_{index}',
                    mysql_conn_id='prd-az1-sqlw2-airflowconnection',
                    sql=f"""
                    UPDATE `clientresults`.`client_hierarchy` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ctp_panel_task(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ctp_panel_{index}',
                    mysql_conn_id='prd-az1-sqlw2-airflowconnection',
                    sql=f"""
                    UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            auto_approval_update_ctp = auto_approval_update_ctp_task()
            auto_approval_update_ch = auto_approval_update_ch_task()
            auto_approval_update_ctp_panel = auto_approval_update_ctp_panel_task()
            
        else:
            print("panel already processed")

with DAG(
    dag_id="csga_panel_auto_approval",
    start_date=datetime(2024,11,6),
    schedule_interval='@hourly',
    tags=['csga_approval','sla_52'],
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="csga_panel_auto_approval_task",
        python_callable=csga_panel_auto_approval_data_pull,
    )
    

    