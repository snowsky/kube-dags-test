from lib.konza.parser import (
    read_clinical_document_from_xml_path, 
    read_clinical_document_base_from_xml_path,
    extract_demographic_info_from_xmls_to_parquet,
)
from lib.konza.extracts.extract import KonzaExtractBase
import os
import tempfile
import pandas as pd


def test_ccda_parsing():
    XML_DIR = "ccda/telecom/"
    for file_name in os.listdir(XML_DIR):
        xml_path = os.path.join(XML_DIR, file_name)
        clinical_document = read_clinical_document_from_xml_path(xml_path)
        assert clinical_document is not None

def test_ccda_basic_extract():
    with tempfile.TemporaryDirectory() as td:
        output_path = os.path.join(td, "output.parquet")
        extract_demographic_info_from_xmls_to_parquet("ccda/telecom/", output_path)
        data = pd.read_parquet(output_path)
        assert list(data['family_name']) == [
            'Jones', 'Jones', 'Jones'
        ]
        assert list(data.keys()) == [
            'file_name', 'name_prefix', 'given_name', 
            'middle_names', 'family_name', 'name_suffix',
            'date_of_birth', 'sex', 'race', 'ethnicity', 'language', 'marital_status',
            'deceased', 'street1', 'street2', 'city', 'state', 'postal_code',
            'emails', 'home_phone_number_id', 'business_phone_number_id', 
            'cell_phone_number_id', 'other_phone_number_id'
        ]

def test_telecom_normal_extract():
    XML_PATH = "ccda/telecom/CCD.xml"
    clinical_document = read_clinical_document_base_from_xml_path(XML_PATH)
    extract = KonzaExtractBase.from_clinical_document(clinical_document)
    assert extract is not None
    
    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == "Isbella.Jones.CCD@gmail.com"
    assert telecom_extract.home_phone_number_id is None
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id == "+1(444)444-4444"
    assert telecom_extract.other_phone_number_id is None


def test_telecom_nouse_noNullFlavor_extract():
    XML_PATH = "ccda/telecom/CCD_telecom_nouse_noNullFlavor.xml"
    clinical_document = read_clinical_document_base_from_xml_path(XML_PATH)
    extract = KonzaExtractBase.from_clinical_document(clinical_document)
    assert extract is not None
    
    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == "Isbella.Jones.CCD@gmail.com"
    assert telecom_extract.home_phone_number_id is None
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id is None
    assert telecom_extract.other_phone_number_id == "+1(444)444-4444"


def test_CCD_telecom_nouse_NullFlavor_extract():
    XML_PATH = "ccda/telecom/CCD_telecom_nouse_NullFlavor.xml"
    clinical_document = read_clinical_document_base_from_xml_path(XML_PATH)
    extract = KonzaExtractBase.from_clinical_document(clinical_document)
    assert extract is not None
    
    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == "Isbella.Jones.CCD@gmail.com"
    assert telecom_extract.home_phone_number_id is None
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id is None
    assert telecom_extract.other_phone_number_id is None