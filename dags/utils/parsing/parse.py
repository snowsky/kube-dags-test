import sys
import os
# Add the dags directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lib.konza.parser import read_clinical_document_from_xml_path
from lib.konza.extracts.extract import KonzaExtract

if __name__ == "__main__":
    for xml_file_name in os.listdir(sys.argv[1])[2:3]:
        xml_path = os.path.join(sys.argv[1], xml_file_name)
        try:
            clinical_document = read_clinical_document_from_xml_path(xml_path)
            extract = KonzaExtract.from_clinical_document(clinical_document)
            print(extract.dump_pipe_delimited())
        except ValueError as e:
            raise ValueError(f"Problem with {xml_path}: {e}")
