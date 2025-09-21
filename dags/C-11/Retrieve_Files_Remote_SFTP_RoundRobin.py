# DAG Version History:
# v1.0 - Initial DAG to move files from SFTP home directory to KONZA_Staging
# v1.1 - Added recursive folder traversal and structure preservation
# v1.2 - Replaced invalid `path_isdir` with `stat.S_ISDIR` using paramiko
# v1.3 - Improved error handling to continue on connection/auth failures
# v1.4 - Added version tracking comments
# v1.5 - Fixed syntax error: missing closing brace in results.append
# v1.6 - Added second round to transfer files from KONZA_Staging to Azure Blob Storage destinations
# v1.7 - Ensured deletion only after successful uploads to both destinations; appended emr_client_name to C-11/L-69/

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
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

AZURE_CONNECTION_NAME = "biakonzasftp-blob-core-windows-net"
CONTAINER_NAME = "airflow"
DESTINATIONS = ["C-126/L-69/", "C-11/L-69/"]

def retrieval_auto_approval_condition_check():
    def move_recursively(sftp_client, source_path, staging_root, relative_path=""):
        items = sftp_client.listdir(source_path)
        for item in items:
            if item in [".", ".."]:
                continue
            item_full_path = os.path.join(source_path, item)
            item_relative_path = os.path.join(relative_path, item)
            staging_path = os.path.join(staging_root, item_relative_path)
            if "KONZA_Staging" in item_full_path:
                continue
            try:
                item_stat = sftp_client.stat(item_full_path)
                if stat.S_ISDIR(item_stat.st_mode):
                    try:
                        sftp_client.mkdir(staging_path)
                    except IOError:
                        pass
                    move_recursively(sftp_client, item_full_path, staging_root, item_relative_path)
                else:
                    sftp_client.rename(item_full_path, staging_path)
                    logging.info(f"Moved file {item_full_path} to {staging_path}")
            except Exception as e:
                logging.error(f"Failed to process {item_full_path}: {e}")

    def transfer_to_azure(sftp_client, staging_root, emr_client_name):
        wasb_hook = WasbHook(wasb_conn_id=AZURE_CONNECTION_NAME)

        def upload_recursively(current_path, relative_path=""):
            items = sftp_client.listdir(current_path)
            for item in items:
                if item in [".", ".."]:
                    continue
                item_full_path = os.path.join(current_path, item)
                item_relative_path = os.path.join(relative_path, item)
                try:
                    item_stat = sftp_client.stat(item_full_path)
                    if stat.S_ISDIR(item_stat.st_mode):
                        upload_recursively(item_full_path, item_relative_path)
                    else:
                        # Download file from SFTP to local temp path
                        local_temp_path = os.path.join("/tmp", item_relative_path)
                        os.makedirs(os.path.dirname(local_temp_path), exist_ok=True)
                        with open(local_temp_path, "wb") as f:
                            f.write(sftp_client.open(item_full_path).read())
        
                        upload_success = []
        
                        for destination in DESTINATIONS:
                            if destination.startswith("C-11/L-69/"):
                                blob_path = os.path.join(destination, emr_client_name, item_relative_path)
                            else:
                                blob_path = os.path.join(destination, item_relative_path)
        
                            try:
                                wasb_hook.load_file(
                                    file_path=local_temp_path,
                                    container_name=CONTAINER_NAME,
                                    blob_name=blob_path,
                                    overwrite=True
                                )
                                logging.info(f"Uploaded {local_temp_path} to Azure Blob Storage as {blob_path}")
                                upload_success.append(True)
                            except Exception as e:
                                logging.error(f"Failed to upload {local_temp_path} to {blob_path}: {e}")
                                upload_success.append(False)
        
                        if all(upload_success):
                            sftp_client.remove(item_full_path)
                            logging.info(f"Deleted {item_full_path} from staging after successful uploads")
                        else:
                            logging.warning(f"File {item_full_path} not deleted due to failed upload(s)")
        
                except Exception as e:
                    logging.error(f"Failed to process {item_full_path}: {e}")


        upload_recursively(staging_root)

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
        logging.info(f"Processing connection ID: {connection_id_md5} for participant: {participant_client_name}")

        try:
            sftp_hook = SFTPHook(ssh_conn_id=connection_id_md5)
        except Exception as e:
            logging.warning(f"Failed ssh_conn_id for {connection_id_md5}: {e}")
            try:
                sftp_hook = SFTPHook(sftp_conn_id=connection_id_md5)
            except Exception as e:
                logging.error(f"Failed sftp_conn_id for {connection_id_md5}: {e}")
                results.append({
                    "connection_id_md5": connection_id_md5,
                    "participant_client_name": participant_client_name,
                    "status": "connection_failed",
                    "error": str(e)
                })
                continue

        try:
            sftp_client = sftp_hook.get_conn()
        except Exception as e:
            logging.error(f"Failed to get SFTP connection for {connection_id_md5}: {e}")
            results.append({
                "connection_id_md5": connection_id_md5,
                "participant_client_name": participant_client_name,
                "status": "auth_failed",
                "error": str(e)
            })
            continue

        root_path = "."
        staging_folder = os.path.join(root_path, "KONZA_Staging")

        try:
            sftp_client.mkdir(staging_folder)
        except IOError:
            logging.info(f"Staging folder may already exist: {staging_folder}")

        try:
            move_recursively(sftp_client, root_path, staging_folder)
            transfer_to_azure(sftp_client, staging_folder, emr_client_name)
            results.append({
                "connection_id_md5": connection_id_md5,
                "participant_client_name": participant_client_name,
                "status": "success"
            })
        except Exception as e:
            logging.error(f"Error during processing for {connection_id_md5}: {e}")
            results.append({
                "connection_id_md5": connection_id_md5,
                "participant_client_name": participant_client_name,
                "status": "processing_failed",
                "error": str(e)
            })

    return results

with DAG(
    dag_id='retrieval_auto_approval_check',
    default_args=default_args,
    description='Check and move files for auto-approved retrieval clients and deliver to Azure Blob Storage',
    schedule_interval='0 */6 * * *',
    max_active_runs=1,
    start_date=datetime(2025, 9, 21),
    catchup=False,
    tags=['C-11'],
) as dag:

    run_check = PythonOperator(
        task_id='run_retrieval_auto_approval_check',
        python_callable=retrieval_auto_approval_condition_check,
    )
