import datetime
import sys
sys.path.insert(0,'/data/reportwriterstorage/lib/ccd-parse-main')
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag, task



@dag(
     dag_id="parse_ccds",
     start_date=datetime.datetime(2021, 1, 1),
     schedule=None,
)
def generate_dag():
     
    @task.virtualenv(
        task_id='parse_xmls',
        requirements=['lxml',
                      'pandas',
                      'pyarrow',
                      'pydantic>2.0',
                      'pydantic-xml',
                      'pytest']
    )
    def parse_xmls():
        import os
        import logging
        logging.info(os.listdir('/source-reportwriterstorage/lib/ccd-parse-main'))
        sys.path.insert(0,'/source-reportwriterstorage/lib/ccd-parse-main')
        from konza.parser import read_clinical_document_from_xml_path
        from konza.extracts.extract import KonzaExtract
        CCDA_DIR = "/source-reportwriterstorage/content/raw_ccds/HL7v3In/"
        for xml_file_name in os.listdir(CCDA_DIR):
            xml_path = os.path.join(CCDA_DIR, xml_file_name)
            try:
                clinical_document = read_clinical_document_from_xml_path(xml_path)
                extract = KonzaExtract.from_clinical_document(clinical_document)
                print(extract.model_dump_json())
            except ValueError as e:
                raise ValueError(f"Problem with {xml_path}: {e}")
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    start >> parse_xmls() >> end
DAG = generate_dag()
