import datetime
import pathlib
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag, task

CCDA_DIR = "/opt/airflow/dags/utils/parsing/ccda"

with DAG(
    'parse_example_ccdas',
    params={
        "ccda_dir": CCDA_DIR
    },
    schedule=None,
    tags=['ccd-parsing'],
) as dag:
   
    from lib.konza.parser import read_clinical_document_from_xml_path
    from lib.konza.extracts.extract import KonzaExtract

    @task
    def list_xmls(ccda_dir):
        return [(x.stem, str(x)) for x in pathlib.Path(ccda_dir).iterdir() if x.is_file()]


    @task(map_index_template="{{ xml_path[0] }}")
    def parse_xml(xml_path, **kwargs):
        logging.info(f"Processing {xml_path[1]}.")
        try:
            clinical_document = read_clinical_document_from_xml_path(xml_path[1])
            extract = KonzaExtract.from_clinical_document(clinical_document)
            print(extract.model_dump_json())
        except ValueError as e:
            raise ValueError(f"Problem with {xml_path}: {e}")

    parsed = parse_xml.expand(xml_path=list_xmls("{{ params.ccda_dir }}"))
