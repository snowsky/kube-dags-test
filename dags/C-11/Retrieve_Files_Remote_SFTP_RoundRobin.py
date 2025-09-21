from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime, timedelta
import logging
import hashlib
import os
import stat

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def retrieval_auto_approval_condition_check():
    def move_recursively(sftp_client, source_path, staging_root, relative_path=""):
        items = sftp_client.listdir(source_path)

        for item in items:
            if item in [".", ".."]:
                continue

            item_full_path = os.path.join(source_path, item)
            item_relative_path = os.path.join(relative_path, item)
            staging_path = os.path.join(staging_root, item_relative_path)

            # Skip the staging folder itself
            if "KONZA_Staging" in item_full_path:
                continue

            try:
                item_stat = sftp_client.stat(item_full_path)
                if stat.S_ISDIR(item_stat.st_mode):
                    try:
                        sftp_client.mkdir(staging_path)
                    except IOError:
                        pass  # Directory may already exist
                    move_recursively(sftp_client, item_full_path, staging_root, item_relative_path)
                else:
                    sftp_client.rename(item_full_path, staging_path)
                    logging.info(f"Moved file {item_full_path} to {staging_path}")
            except Exception as e:
                logging.error(f"Failed to process {item_full_path}: {e}")

    pg_hook = PostgresHook(postgres_conn_id="prd-az1-ops3-airflowconnection")

    query = """
        SELECT emr_client_name, authorized_identifier, participant_client_name
        FROM cda_konza_sftp_retrieval__l_69
        WHERE emr_client_delivery_paused = '0';
    """
    df_retrieve_auto_approved = pg_hook.get_pandas_df(query)

    df_retrieve_auto_approved["connection_id_md5"] = df_retrieve_auto_approved["emr_client_name"].apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest() if isinstance(x, str) else None
    )

    if df_retrieve_auto_approved.empty:
        logging.info("No clients to process.")
        return []

    results = []

    for _, row in df_retrieve_auto_approved.iterrows():
        connection_id_md5 = row['connection_id_md5']
        emr_client_name = row['emr_client_name']
        participant_client_name = row['participant_client_name']
        logging.info(f"Processing connection ID: {connection_id_md5} for participant: {participant_client_name} using SFTP configuration from {emr_client_name}")

        try:
            sftp_hook = SFTPHook(ssh_conn_id=connection_id_md5)
        except Exception as e:
            logging.warning(f"Failed ssh_conn_id: {e}")
            try:
                sftp_hook = SFTPHook(sftp_conn_id=connection_id_md5)
            except Exception as e:
                logging.error(f"Failed sftp_conn_id: {e}")
                results.append({
                    "connection_id_md5": connection_id_md5,
                    "participant_client_name": participant_client_name,
                    "status": "failed",
                    "error": str(e)
                })
                continue

        sftp_client = sftp_hook.get_conn()
        root_path = "."
        staging_folder = os.path.join(root_path, "KONZA_Staging")

        try:
            sftp_client.mkdir(staging_folder)
        except IOError:
            logging.info(f"Staging folder may already exist: {staging_folder}")

        try:
            move_recursively(sftp_client, root_path, staging_folder)
            results.append({
                "connection_id_md5": connection_id_md5,
                "participant_client_name": participant_client_name,
                "status": "success"
            })
        except Exception as e:
            logging.error(f"Error during recursive move: {e}")
            results.append({
                "connection_id_md5": connection_id_md5,
                "participant_client_name": participant_client_name,
                "status": "error",
                "error": str(e)
            })

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
