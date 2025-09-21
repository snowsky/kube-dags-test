from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime, timedelta
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def retrieval_auto_approval_condition_check():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="prd-az1-ops3-airflowconnection")

    query = """
        SELECT md5(emr_client_name) AS connection_id_md5, authorized_identifier, participant_client_name, folder_name
        FROM cda_konza_sftp_retrieval__l_69
        WHERE emr_client_delivery_paused = 0;
    """
    df_panel_auto_approved = pg_hook.get_pandas_df(query)

    if df_panel_auto_approved.empty:
        logging.info("No clients to process.")
        return []

    results = []

    for _, row in df_panel_auto_approved.iterrows():
        connection_id_md5 = row['connection_id_md5']
        client_reference_folder = row['folder_name']
        logging.info(f"Processing connection ID: {connection_id_md5} for folder: {client_reference_folder}")

        try:
            sftp_hook = SFTPHook(ssh_conn_id=connection_id_md5)
        except Exception as e:
            logging.warning(f"Failed ssh_conn_id: {e}")
            try:
                sftp_hook = SFTPHook(sftp_conn_id=connection_id_md5)
            except Exception as e:
                logging.error(f"Failed sftp_conn_id: {e}")
                continue

        try:
            files = sftp_hook.list_path(client_reference_folder)
            staging_folder = os.path.join(client_reference_folder, "KONZA_Staging")

            try:
                sftp_hook.create_directory(staging_folder)
            except Exception as e:
                logging.info(f"Staging folder may already exist: {e}")

            for file in files:
                file_path = os.path.join(client_reference_folder, file)
                staging_path = os.path.join(staging_folder, file)

                try:
                    sftp_hook.rename(file_path, staging_path)
                    logging.info(f"Moved {file_path} to {staging_path}")
                except Exception as e:
                    logging.error(f"Failed to move {file_path}: {e}")

        except Exception as e:
            logging.error(f"Error processing folder {client_reference_folder}: {e}")

    return results

with DAG(
    dag_id='retrieval_auto_approval_check',
    default_args=default_args,
    description='Check and move files for auto-approved retrieval clients',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    max_active_runs=1,
    start_date=datetime(2025, 9, 21),
    catchup=False,
    tags=['C-11'],
) as dag:

    run_check = PythonOperator(
        task_id='run_retrieval_auto_approval_check',
        python_callable=retrieval_auto_approval_condition_check,
    )
