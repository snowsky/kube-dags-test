import datetime
import sys
sys.path.insert(0,'/data/reportwriterstorage/lib/ccd-parse-main')
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.decorators import dag, task

CCDA_DIR = "/data/reportwriterstorage/content/raw_ccds/HL7v3In/"

@dag(
     dag_id="parse_ccds",
     start_date=datetime.datetime(2021, 1, 1),
     schedule="@daily",
)
def generate_dag():
     
    @task
    def parse_xmls():
        from konza.parser import read_clinical_document_from_xml_path
        from konza.extract import KonzaExtract
        for xml_file_name in os.listdir(CCDA_DIR):
            xml_path = os.path.join(sys.argv[1], xml_file_name)
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
