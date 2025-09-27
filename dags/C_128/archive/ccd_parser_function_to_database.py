import pathlib
import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook

from lib.konza.parser import read_clinical_document_from_xml_path
from lib.konza.extracts.extract import KonzaExtract

CCDA_DIR = "/data/biakonzasftp/C-128/archive/XCAIn"
DEFAULT_DB_CONN_ID = 'prd-az1-sqlw3-mysql-airflowconnection'

with DAG(
    'XCAIn_parse_EUID',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
    },
    params={
        "ccda_dir": CCDA_DIR
    },
    schedule=None,
    tags=['C-128'],
    concurrency=5,
    catchup=False,
) as dag:

    sql_hook = MySqlHook(mysql_conn_id=DEFAULT_DB_CONN_ID)

    @task
    def list_all_files(ccda_dir):
        files = [
            str(x)
            for x in pathlib.Path(ccda_dir).rglob("*")
            if x.is_file()
        ]
        logging.info(f"Total files found: {len(files)}")
        return files

    @task
    def filter_and_process_xml_files(files):
        xml_files = [(pathlib.Path(f).stem, f) for f in files if f.endswith(".xml")]
        logging.info(f"Filtered XML files count: {len(xml_files)}")

        for stem, path in xml_files:
            try:
                logging.info(f"Processing file: {path}")
                clinical_document = read_clinical_document_from_xml_path(path)
                extract = KonzaExtract.from_clinical_document(clinical_document)

                given_name_parts = extract.patient_name_extract.given_name.split()
                family_name_parts = extract.patient_name_extract.family_name.split()

                firstname = given_name_parts[0] if given_name_parts else ''
                middlename = given_name_parts[1] if len(given_name_parts) > 1 else ''
                lastname = family_name_parts[0] if family_name_parts else ''
                mrn = '999999999'
                event_timestamp = '2023-02-08 10:34:00'
                euid = stem

                insert_query = f"""
                    INSERT INTO person_master._mpi 
                    (mrn, firstname, middlename, lastname, event_timestamp, euid)
                    VALUES 
                    ('{mrn}', '{firstname}', '{middlename}', '{lastname}', '{event_timestamp}', '{euid}');
                """
                sql_hook.run(insert_query)
                logging.info(f"Inserted record for EUID: {euid}")

                file_key = pathlib.Path(path).name
                mark_query = f"""
                    INSERT INTO archive.processed_files (file_key)
                    VALUES ('{file_key}');
                """
                sql_hook.run(mark_query)
                logging.info(f"Marked file as processed: {file_key}")

            except Exception as e:
                logging.error(f"Failed to process file {path}: {e}")
                # Optional: raise if you want DAG to fail
                continue

    # DAG task flow
    all_files = list_all_files(CCDA_DIR)
    filter_and_process_xml_files(all_files)
