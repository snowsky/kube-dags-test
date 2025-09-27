from __future__ import annotations
import os
import sys
import time
import subprocess
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
import paramiko
from pyunpack import Archive

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

from airflow.providers.sftp.hooks.sftp import SFTPHook
import hashlib

# -----------------------
# Constants / Settings
# -----------------------
SCRIPT_NAME = "CDA_KONZA_SFTP_Retrieval__L_69"

# Airflow connection IDs
PG_CONN_ID_OPS3 = "prd-az1-ops3-airflowconnection"  # sourceoftruth (OPS3)
#PG_CONN_ID_MGMTOPS = " "             # =sftp_credentials_parent_mgmt
MSSQL_CONN_ID_FORMS = "formoperations_prd_az1_opssql_database_windows_net"        # MSSQL for outgoing_emails

# 7-zip detection (fallback to your legacy path if not found)
SEVENZ_PATH_CANDIDATES = ["/usr/bin/7za", r"C:\7z1900-extra\7za.exe"]


# UNC destination roots (same as legacy)
DESTPATH1_ROOT = r"\\PRD-AZ1-SQLW2\HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69"
DESTPATH2_ROOT = r"\\prd-az1-ops2\CCD-IN-CloverDX-1"
DESTPATH2_ZIPPROC_ROOT = r"\\prd-az1-ops2\L_69_processing"

# General DAG defaults
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,  # you can bump if desired
}

def _detect_7za() -> str | None:
    for p in SEVENZ_PATH_CANDIDATES:
        if os.path.exists(p):
            return p
    return None

def _mssql_send_alerts(subject: str, body: str) -> None:
    """
    Insert alert emails into formoperations.outgoing_emails as in legacy.
    Sends to tlamond@konza.org and swarnock@konza.org.
    """
    try:
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID_FORMS)
        with hook.get_conn() as conn:
            cur = conn.cursor()
            for recipient in ("tlamond@konza.org", "swarnock@konza.org"):
                cur.execute(
                    "INSERT INTO outgoing_emails (receipient, email_subject, email_body) VALUES (%s, %s, %s)",
                    (recipient, subject, body),
                )
            conn.commit()
    except Exception as e:
        logging.warning("MSSQL alert insert failed: %s", e)


def _unzip_with_fallback(zip_path: str, extract_dir: str) -> bool:
    """
    Try pyunpack first; fallback to 7za. Returns True if successful, else False.
    """
    try:
        Archive(zip_path).extractall(extract_dir)
        return True
    except Exception as e:
        logging.warning("pyunpack failed for %s: %s. Falling back to 7za.", zip_path, e)

    seven = _detect_7za()
    if not seven:
        logging.error("7za not found; cannot extract %s", zip_path)
        return False

    try:
        cmd = [seven, "x", zip_path, "-aou", extract_dir]
        output = subprocess.run(cmd, capture_output=True, text=True)
        logging.info("7za output: %s", output.stdout or output.stderr)
        return output.returncode == 0
    except Exception as e:
        logging.error("7za subprocess failed: %s", e)
        return False

with DAG(
    dag_id=SCRIPT_NAME,
    description="Airflow migration of L-69 SFTP retrieval from legacy OPS1 script",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["L-69"],
) as dag:

    @task()
    def get_restart_trigger() -> List[Dict]:
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3)
        df = hook.get_pandas_df("SELECT * FROM restart_trigger;")
        if df.empty:
            raise AirflowFailException("restart_trigger is empty.")
        return df.to_dict(orient="records")

    @task()
    def get_schedule_jobs() -> List[Dict]:
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3)
        df = hook.get_pandas_df("SELECT * FROM schedule_jobs;")
        if df.empty:
            raise AirflowFailException("schedule_jobs is empty.")
        return df.to_dict(orient="records")

    @task()
    def check_schedule_switch(restart_trigger_data: List[Dict], schedule_jobs_data: List[Dict]) -> bool:
        df_restart = pd.DataFrame(restart_trigger_data)
        df_jobs = pd.DataFrame(schedule_jobs_data)
        df_jobs_prep = df_jobs[df_jobs["script_name"] == SCRIPT_NAME].copy()
        df_jobs_prep["trigger_name"] = df_jobs_prep["server"]
        merged = df_jobs_prep.merge(df_restart, on="trigger_name", how="inner")
        if merged.empty:
            raise AirflowFailException("No matching job between schedule_jobs and restart_trigger.")
        if int(merged["switch"].iloc[0]) != 0:
            raise AirflowSkipException(f"Schedule switch OFF for {SCRIPT_NAME}.")
        return True

    @task()
    def check_overlap_flag() -> bool:
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3)
        df = hook.get_pandas_df(
            f"SELECT * FROM job_triggers WHERE associated_table = '{SCRIPT_NAME}';"
        )
        if df.empty:
            raise AirflowFailException(f"No job_triggers row for {SCRIPT_NAME}")
        if int(df["trigger_status"].iloc[0]) != 0:
            raise AirflowSkipException(f"Job already running for {SCRIPT_NAME}")
        return True

    @task()
    def set_overlap_flag() -> str:
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3)
        hook.run(
            f"UPDATE job_triggers SET trigger_status = '1' WHERE associated_table = '{SCRIPT_NAME}';"
        )
        return "trigger_status set to 1"

    @task()
    def get_authorized_clients() -> List[Dict]:
        """
        SELECT emr_client_name, emr_client_delivery_paused, source_files_zipped
        GROUP BY ...
        """
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3)
        sql = """
            SELECT emr_client_name, emr_client_delivery_paused, source_files_zipped
            FROM cda_konza_sftp_retrieval__l_69
            GROUP BY emr_client_name, emr_client_delivery_paused, source_files_zipped;
        """
        df = hook.get_pandas_df(sql)
        if df.empty:
            raise AirflowFailException("No clients in cda_konza_sftp_retrieval__l_69.")
        df = df[df["emr_client_delivery_paused"] == "0"]
        if df.empty:
            raise AirflowSkipException("All clients paused.")
        return df.to_dict(orient="records")

    @task()
    def process_client(client: Dict) -> str:
        """
        Full legacy logic per client (SFTP connect w/ retries, staging download, unzip, copy, cleanup, move to staging).
        """
        titleString = SCRIPT_NAME
        try:
            folder_name = client["emr_client_name"]
            zip_option = str(client.get("source_files_zipped", "0"))
            folder_name_dh = folder_name.replace(" ", ".")
            today_marker = datetime.today().strftime("%Y%m%d")

            # 1) Fetch SFTP credentials from MGMTOPS
            connection_id_md5 = hashlib.md5(folder_name.encode()).hexdigest()
            try:
                sftp_hook = SFTPHook(ssh_conn_id=connection_id_md5)
            except Exception as e:
                logging.error("No Airflow SFTP connection found for %s (%s)", folder_name, connection_id_md5)
                return f"SKIPPED: {folder_name}"
            
            # Pull root directory (optional: store in extra of Airflow connection)
            dbcred_sftp_root_directory = "/incoming/"  # or fetch from Airflow conn extras
            sourcePath = dbcred_sftp_root_directory
            sourcePathStaging = os.path.join(dbcred_sftp_root_directory, "KONZA_Staging/")
            destPath1 = os.path.join(DESTPATH1_ROOT, folder_name)
            today_marker = datetime.today().strftime("%Y%m%d")
            destPath2 = os.path.join(DESTPATH2_ROOT, today_marker)
            destPath2ZipProcessing = os.path.join(DESTPATH2_ZIPPROC_ROOT, today_marker) 

            # Paths (same as legacy)
            sourcePath = dbcred_sftp_root_directory
            sourcePathStaging = dbcred_sftp_root_directory + "KONZA_Staging/"
            destPath1 = os.path.join(DESTPATH1_ROOT, folder_name)
            destPath2 = os.path.join(DESTPATH2_ROOT, today_marker)
            destPath2ZipProcessing = os.path.join(DESTPATH2_ZIPPROC_ROOT, today_marker) 

            # 2) Connect SFTP with retries
            try:
                sftp = sftp_hook.get_conn()
            except Exception as e:
                logging.error("SFTP connection failed for %s: %s", folder_name, e)
                return f"FAILED_CONNECT: {folder_name}"

            # ensure staging exists
            try:
                sftp.listdir(sourcePathStaging)
            except IOError:
                try:
                    sftp.mkdir(sourcePathStaging)
                except Exception:
                    pass

            # Get top-level directories
            mainLayer1dir = sftp.listdir(sourcePath)
            layer1Stagingdir = sftp.listdir(sourcePathStaging)

            # 3) Download ALL files from staging to destPath1 + (destPath2/destPath2ZipProcessing)
            dFileCounter = 0
            dprintcount = 0
            for d in layer1Stagingdir:
                output_ok = True  # to mimic "output = 'returncode=0'" guard
                layer2 = sftp.listdir(f"{sourcePathStaging}{d}")
                for d2 in layer2:
                    if d2 == "KONZA_Staging":
                        continue

                    if dprintcount < 5:
                        logging.info("Downloading from Staging - %s", f"{sourcePathStaging}{d}/{d2}")
                        dprintcount += 1

                    # destPath1/<today>/<d>/
                    dest1_dir = os.path.join(destPath1, today_marker, d)
                    os.makedirs(dest1_dir, exist_ok=True)

                    # fetch to dest1
                    sftp.get(f"{sourcePathStaging}{d}/{d2}", os.path.join(dest1_dir, d2))

                    if zip_option == "1":
                        processingPath = os.path.join(dest1_dir, d2)
                        extract_dir = processingPath.replace(".zip", "")
                        os.makedirs(extract_dir, exist_ok=True)
                        if not _unzip_with_fallback(processingPath, extract_dir):
                            # email alerts for zip bypass
                            subj = f"The L-69 script has bypassed for {destPath1} zip file and data is not being processed"
                            body = f"The L-69 script bypassed this client zip file and data is not being processed {processingPath}"
                            _mssql_send_alerts(subject=subj, body=body)
                            output_ok = False
                        else:
                            try:
                                os.remove(processingPath)
                            except Exception:
                                pass

                    # also deliver to dest2
                    os.makedirs(os.path.join(destPath2, d), exist_ok=True)
                    if zip_option != "1":
                        sftp.get(f"{sourcePathStaging}{d}/{d2}", os.path.join(destPath2, d, d2))
                    else:
                        os.makedirs(os.path.join(destPath2ZipProcessing, d), exist_ok=True)
                        sftp.get(f"{sourcePathStaging}{d}/{d2}", os.path.join(destPath2ZipProcessing, d, d2))

                        processingPathStep1 = os.path.join(destPath2ZipProcessing, d, d2)
                        processingPath2 = os.path.join(destPath2, d, d2)
                        extract_dir2 = processingPath2.replace(".zip", "")
                        os.makedirs(extract_dir2, exist_ok=True)
                        if not _unzip_with_fallback(processingPathStep1, extract_dir2):
                            subj = f"The L-69 script has bypassed for {destPath1} zip file and data is not being processed"
                            body = f"The L-69 script bypassed this client zip file and data is not being processed {processingPath2}"
                            _mssql_send_alerts(subject=subj, body=body)
                            output_ok = False
                        else:
                            try:
                                os.remove(processingPathStep1)
                            except Exception:
                                pass

                    dFileCounter += 1

            logging.info("Total Files Downloaded to Destination(s): %s", dFileCounter)
            sftp.close()

            # Reconnect for cleanup steps
            try:
                sftp = sftp_hook.get_conn()
            except Exception as e:
                logging.error("SFTP reconnect failed for %s (%s): %s", folder_name, connection_id_md5, e)
                return f"FAILED_RECONNECT_1: {folder_name}"
            
            # 4) Remove all files from staging
            sFileCounter = 0
            sprintcount = 0
            for d in layer1Stagingdir:
                layer2 = sftp.listdir(f"{sourcePathStaging}{d}")
                for d2 in layer2:
                    if d2 == "KONZA_Staging":
                        continue
                    if sprintcount < 5:
                        logging.info("Removing - %s", f"{sourcePathStaging}{d}/{d2}")
                        sprintcount += 1
                    try:
                        sftp.remove(f"{sourcePathStaging}{d}/{d2}")
                    except Exception as e:
                        logging.warning("Failed to remove staging file: %s", e)
                    sFileCounter += 1
            
            logging.info("Total Files Removed from Staging: %s", sFileCounter)
            sftp.close()


            # 5) Transfer from source to staging for authorized IDs (post-download restage)
            #    First, refetch authorized rows including authorized_identifier for this client
            df_authorized = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3).get_pandas_df(
                "SELECT * FROM cda_konza_sftp_retrieval__l_69 WHERE emr_client_name = %s AND emr_client_delivery_paused = '0';",
                parameters=(folder_name,)
            )
            if df_authorized.empty:
                logging.info("No authorized rows for %s during restage pass.", folder_name)
                return f"OK: {folder_name} (no authorized rows at restage)"

            # Build a set of authorized identifiers (mirror legacy)
            if "authorized_identifier" in df_authorized.columns:
                authorized_ids = set(df_authorized["authorized_identifier"].dropna().astype(str).tolist())
            else:
                # If missing column, restage everything found under sourcePath except KONZA_Staging
                authorized_ids = None

            # Reconnect again for restaging (authorized move from source to staging)
            try:
                sftp = sftp_hook.get_conn()
            except Exception as e:
                logging.error("SFTP reconnect failed during restage for %s (%s): %s", folder_name, connection_id_md5, e)
                return f"FAILED_RECONNECT_2: {folder_name}"

            # Refresh main layer listing
            mainLayer1dir = sftp.listdir(sourcePath)
            tFileCounter = 0
            tprintcount = 0

            for sdir in mainLayer1dir:
                if sdir == "KONZA_Staging":
                    continue
                if (authorized_ids is None) or (sdir in authorized_ids):
                    mainLayer2dir = sftp.listdir(f"{sourcePath}{sdir}")
                    for d2 in mainLayer2dir:
                        if tprintcount < 5:
                            logging.info("Renaming - %s", f"{sourcePath}{sdir}/{d2}")
                            tprintcount += 1
                        # ensure staging subdir exists
                        try:
                            sftp.chdir(f"{sourcePathStaging}{sdir}/")
                        except IOError:
                            try:
                                sftp.mkdir(f"{sourcePathStaging}{sdir}/")
                                sftp.chdir(f"{sourcePathStaging}{sdir}/")
                            except Exception:
                                pass
                        # move from source to staging
                        try:
                            sftp.rename(f"{sourcePath}{sdir}/{d2}", f"{sourcePathStaging}{sdir}/{d2}")
                            tFileCounter += 1
                        except Exception as e:
                            logging.warning("sftp.rename failed: %s", e)

            logging.info("Total Files Moved to Staging: %s", tFileCounter)
            sftp.close()

            return f"OK: {folder_name}"


            return f"OK: {folder_name}"

        except Exception as e:
            # legacy-style error handling (write to file + FORMS emails)
            exc_info = sys.exc_info()
            line_no = ""
            try:
                line_no = str(exc_info[2].tb_lineno) if exc_info[2] else ""
            except Exception:
                pass

            err_str = f"{e}"
            logging.error("Unhandled error (Line %s): %s", line_no, err_str)

            try:
                subj = f"The L-69 script has bypassed for {client.get('emr_client_name','(unknown)')} entirely due to error"
                body = f"Error Reported: {err_str.replace(\"'\",\"\")}"
                _mssql_send_alerts(subj, body)
            except Exception as ee:
                logging.warning("Sending MSSQL alerts failed: %s", ee)

            return f"FAILED: {client.get('emr_client_name','(unknown)')}"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def reset_overlap_flag() -> str:
        # Always reset the flag even if upstream clients failed
        try:
            hook = PostgresHook(postgres_conn_id=PG_CONN_ID_OPS3)
            hook.run(
                f"UPDATE job_triggers SET trigger_status = '0' WHERE associated_table = '{SCRIPT_NAME}';"
            )
            return "trigger_status reset to 0"
        except Exception as e:
            logging.error("Failed to reset trigger_status: %s", e)

            # Do not fail the DAG here; just log
            return "trigger_status reset attempt logged"

    # ========= DAG FLOW =========
    restart_trigger_data = get_restart_trigger()
    schedule_jobs_data = get_schedule_jobs()
    switch_ok = check_schedule_switch(restart_trigger_data, schedule_jobs_data)
    overlap_ok = check_overlap_flag()
    flag_on = set_overlap_flag()

    clients = get_authorized_clients()
    results = process_client.expand(client=clients)

    flag_off = reset_overlap_flag()

    # Dependencies (mirror legacy order)
    [restart_trigger_data, schedule_jobs_data] >> switch_ok >> overlap_ok >> flag_on >> clients >> results >> flag_off
