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
    sql_hook = MySqlHook(mysql_conn_id="prd-az1-sqlw3-mysql-airflowconnection")  # Replace with your connection ID
    sql_hook_old = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")  # Replace with your connection ID
    
    # Check against database entry in production W3 CSGA
    query = "SELECT * FROM _dashboard_requests.clients_to_process_panel_audit;"  # Replace with your SQL query
    dfPanelAudit = sql_hook.get_pandas_df(query)
    if dfPanelAudit.empty: #Use the old DB W2 if needed
        query = "SELECT * FROM _dashboard_requests.clients_to_process_panel_audit;"  # Replace with your SQL query
        dfPanelAudit = sql_hook_old.get_pandas_df(query)

    ds = kwargs['ds']
    logging.info(f'DS: {ds}')
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
            max_modified_time = None
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
                        max_modified_time = modified_time
                if max_modified_time:
                    logging.info(f'Max Modified Date: {max_modified_time}')
                    
        
        except Exception as e:
            logging.error(f'Error Occurred: {e}')
            
        # Check against database entry in production W3 CSGA
        db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings  WHERE md5(Client) = '{client_md5}' LIMIT 1"
        dfCurrentCSG = sql_hook.get_pandas_df(db_query)
        if dfCurrentCSG.empty: #Use the old DB W2 if needed
            db_query = f"select Client, event_timestamp, md5(Client) as md5 from clientresults.client_security_groupings  WHERE md5(Client) = '{client_md5}' LIMIT 1"
            dfCurrentCSG = sql_hook_old.get_pandas_df(db_query)
        modified_time = dfCurrentCSG['event_timestamp'].iloc[0]

##########################################################Older Stuff Below
    
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
                    mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection',
                    sql=f"""
                    UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ch_task(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ch_{index}',
                    mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection',
                    sql=f"""
                    UPDATE `clientresults`.`client_hierarchy` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ctp_panel_task(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ctp_panel_{index}',
                    mysql_conn_id='prd-az1-sqlw3-mysql-airflowconnection',
                    sql=f"""
                    UPDATE `_dashboard_requests`.`clients_to_process_panel_audit` SET `most_recent_filename`='{latestfile}', `most_recent_file_timestamp`='{latest_time}' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ctp_task_old(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ctp_{index}',
                    mysql_conn_id='prd-az1-sqlw2-airflowconnection',
                    sql=f"""
                    UPDATE `_dashboard_requests`.`clients_to_process` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ch_task_old(**kwargs):
                mysql_op = MySqlOperator(
                    task_id=f'auto_approval_update_ch_{index}',
                    mysql_conn_id='prd-az1-sqlw2-airflowconnection',
                    sql=f"""
                    UPDATE `clientresults`.`client_hierarchy` SET `frequency`='Approved' WHERE `folder_name`='{folder_name}';
                    """,
                    dag=dag
                )
                mysql_op.execute(dict())
            def auto_approval_update_ctp_panel_task_old(**kwargs):
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
            auto_approval_update_ctp_old = auto_approval_update_ctp_task_old()
            auto_approval_update_ch_old = auto_approval_update_ch_task_old()
            auto_approval_update_ctp_panel_old = auto_approval_update_ctp_panel_task_old()
            
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
    

    
