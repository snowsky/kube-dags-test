import pathlib
import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import SQLExecuteQueryOperator
from airflow.decorators import task

CCDA_DIR = "/data/biakonzasftp/C-128/archive/XCAIn"

with DAG(
    'XCAIn_C-128_parse_CCD',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
    },
    params={
        "ccda_dir": CCDA_DIR
    },
    schedule=None,
    tags=['C-128'],
    concurrency=5,  # Set the maximum number of tasks that can run concurrently
) as dag:
    from lib.konza.parser import read_clinical_document_from_xml_path
    from lib.konza.extracts.extract import KonzaExtract

    @task
    def list_xmls(ccda_dir):
        files = [(x.stem, str(x)) for x in pathlib.Path(ccda_dir).iterdir() if x.is_file()]
        logging.info(f"Files found: {files}")
        return files

    @task
    def parse_xml(xml_path):
        logging.info(f"Processing {xml_path}.")
        try:
            stem, path = xml_path  # Unpack the tuple
            clinical_document = read_clinical_document_from_xml_path(path)
            extract = KonzaExtract.from_clinical_document(clinical_document)
            euid = stem

            # Log the actual values of given_name and family_name
            given_name = extract.patient_name_extract.given_name
            family_name = extract.patient_name_extract.family_name
            logging.info(f"Given name: {given_name}")
            logging.info(f"Family name: {family_name}")

            # Ensure the name splits correctly
            given_name_parts = given_name.split()
            family_name_parts = family_name.split()

            logging.info(f"Given name parts: {given_name_parts}")
            logging.info(f"Family name parts: {family_name_parts}")

            firstname = given_name_parts[0]
            middlename = given_name_parts[1] if len(given_name_parts) > 1 else ''
            lastname = family_name_parts[0]

            mrn = '999999999'
            event_timestamp = '2023-02-08 10:34:00'
            print(extract.model_dump_json())
            logging.info(f'First name should be {extract.patient_name_extract}')
            logging.info(f'First name should be {extract.patient_name_extract.given_name}')
            logging.info(f'EUID to SQL should be {euid}')
            logging.info(f'mrn to SQL should be {mrn}')
            logging.info(f'event_timestamp to SQL should be {event_timestamp}')
            mysql_op = SQLExecuteQueryOperator(
                task_id='parse_ccd_to_sql',
                #conn_id='qa-az1-sqlw3-airflowconnection',
                conn_id='prd-az1-sqlw3-mysql-airflowconnection',
                sql=f"""
                insert into person_master._mpi (mrn, firstname, middlename, lastname, event_timestamp, euid)
                VALUES ('{mrn}', '{firstname}', '{middlename}', '{lastname}', '{event_timestamp}', '{euid}') ;
                """,
                dag=dag,
                pool='ccd_pool'  # Use the pool defined in Airflow
            )
            mysql_op.execute(dict())
        except ValueError as e:
            raise ValueError(f"Problem with {xml_path}: {e}")
        except IndexError as e:
            raise IndexError(f"IndexError with {xml_path}: {e}")

    xml_files = list_xmls(CCDA_DIR)
    parse_xml.expand(xml_path=xml_files)
