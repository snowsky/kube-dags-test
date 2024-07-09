from pydantic_xml import BaseXmlModel, element, attr, RootXmlModel, wrapped
from lxml.etree import _Element as Element
from typing import Optional, List
from pydantic import model_validator, field_validator
import pathlib
from konza.utils import parser_other
from konza.common import PYXML_KWARGS
from konza.extracts.extract import KonzaExtractBase
from konza.clinical_document import ClinicalDocument, ClinicalDocumentBase
import os
import pandas as pd
from pathlib import Path


def read_clinical_document_from_xml_path(xml_path: str):
    xml_doc = pathlib.Path(xml_path).read_bytes()
    return ClinicalDocument.from_xml(xml_doc)

def read_clinical_document_base_from_xml_path(xml_path: str):
    xml_doc = pathlib.Path(xml_path).read_bytes()
    return ClinicalDocumentBase.from_xml(xml_doc)

def extract_demographic_info_from_xmls_to_parquet(
    input_xmls_dir_path: str, 
    output_parquet_file_path: str,
):
    
    rows = []
    for xml_path in list(Path(input_xmls_dir_path).rglob("*.[xX][mM][lL]")):
        xml_doc = pathlib.Path(xml_path).read_bytes()
        clinical_document = ClinicalDocumentBase.from_xml(xml_doc)
        extract = KonzaExtractBase.from_clinical_document(clinical_document)
        row = {}
        row['file_name'] = os.path.split(xml_path)[-1]
        row = {**row, **extract.patient_name_extract.model_dump()}
        row = {**row, **extract.patient_demographic_extract.model_dump()}
        row = {**row, **extract.patient_address_extract.model_dump()}
        row = {**row, **extract.patient_telecom_extract.model_dump()}
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_parquet(output_parquet_file_path)

