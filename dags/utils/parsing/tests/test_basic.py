from konza.parser import (
    read_clinical_document_from_xml_path, 
    read_clinical_document_base_from_xml_path,
    extract_demographic_info_from_xmls_to_parquet,
)
from konza.extracts.extract import KonzaExtract, KonzaExtractBase
from konza.extracts.problem_extract import ProblemExtract
from konza.extracts.problem_observation_extract import ProblemObservationExtract
from konza.extracts.procedure_extract import ProcedureExtract
import os
import tempfile
import pandas as pd
from pathlib import Path


def test_ccda_parsing():
    XML_DIR = "ccda/"
    for xml_path in list(Path(XML_DIR).rglob("*.[xX][mM][lL]")):
        clinical_document = read_clinical_document_from_xml_path(xml_path)
        assert clinical_document is not None

def test_ccda_basic_extract():
    with tempfile.TemporaryDirectory() as td:
        output_path = os.path.join(td, "output.parquet")
        extract_demographic_info_from_xmls_to_parquet("ccda/", output_path)
        data = pd.read_parquet(output_path)
        assert list(data['family_name']) == [
            'Jones', 'Maur', 'Stewart', 'Everyman', 'Williamson', 'Jones',
            'Everywoman', 'Dare640', 'Everywoman', 'ClinicalSummary', 'Everyman', 'DEMO',
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

def test_basic_extract():
    XML_PATH = "ccda/CCD.xml"
    clinical_document = read_clinical_document_base_from_xml_path(XML_PATH)
    extract = KonzaExtractBase.from_clinical_document(clinical_document)
    assert extract is not None

    name_extract = extract.patient_name_extract
    assert name_extract.name_prefix is None
    assert name_extract.given_name == "Isabella"
    assert name_extract.middle_names == ""
    assert name_extract.family_name == "Jones"
    assert name_extract.name_suffix is None

    address_extract = extract.patient_address_extract
    assert address_extract.street1 == "4567 Residence Rd"
    assert address_extract.street2 == ""
    assert address_extract.city == "Beaverton"
    assert address_extract.state == "OR"
    assert address_extract.postal_code == "97867"

    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == "Isbella.Jones.CCD@gmail.com"
    assert telecom_extract.home_phone_number_id is None
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id == "+1(444)444-4444"

    demographic_extract = extract.patient_demographic_extract
    assert demographic_extract.date_of_birth == "19501219"
    assert demographic_extract.sex == "F"
    assert demographic_extract.race == "2106-3"
    assert demographic_extract.ethnicity == "2186-5"
    assert demographic_extract.language == "ita"
    assert demographic_extract.marital_status == "M"
    assert not demographic_extract.deceased


def test_deceased_extract():
    XML_PATH = "ccda/deceased.xml"
    clinical_document = read_clinical_document_from_xml_path(XML_PATH)
    extract = KonzaExtract.from_clinical_document(clinical_document)
    assert extract is not None

    name_extract = extract.patient_name_extract
    assert name_extract.name_prefix is None
    assert name_extract.given_name == "Eve"
    assert name_extract.middle_names == ""
    assert name_extract.family_name == "Everywoman"
    assert name_extract.name_suffix is None

    address_extract = extract.patient_address_extract
    assert address_extract.street1 == "2222 Home Street"
    assert address_extract.street2 == ""
    assert address_extract.city == "Beaverton"
    assert address_extract.state == "OR"
    assert address_extract.postal_code == "97867"

    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == ""
    assert telecom_extract.home_phone_number_id == "+1(555)555-2003"
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id is None

    demographic_extract = extract.patient_demographic_extract
    assert demographic_extract.date_of_birth == "19730531"
    assert demographic_extract.sex == "F"
    assert demographic_extract.race == "2106-3"
    assert demographic_extract.ethnicity == "2186-5"
    assert demographic_extract.language == "en"
    assert demographic_extract.marital_status == "M"
    assert demographic_extract.deceased

    assert len(extract.insurance_extracts) == 1
    
    import logging
    logging.error(extract.problem_extracts)
    assert extract.problem_extracts == [
        ProblemExtract(observations=[
            ProblemObservationExtract(
                code='233604007', 
                display_name='Pneumonia', 
                description=None, 
                provider_id='555555555', 
                diagnosis_timestamp='200808141030-0800'
            )
        ]), 
        ProblemExtract(observations=[
            ProblemObservationExtract(
                code='29857009', display_name='Chest pain', 
                description=None, provider_id='555555555', 
                diagnosis_timestamp='200704141515-0800'
            ), ProblemObservationExtract(
                code='194828000', display_name='Angina', 
                description=None, provider_id='222334444', 
                diagnosis_timestamp='200704171515-0800'
            )
        ]), 
        ProblemExtract(observations=[
            ProblemObservationExtract(
                code='233604007', display_name='Pneumonia', 
                description=None, provider_id='555555555', 
                diagnosis_timestamp='199803161030-0800'
            )
        ])
    ]
    assert extract.procedure_extracts == [
        ProcedureExtract(
            code='73761001', 
            display_name='Colonoscopy', 
            provider_id=None, 
            original_text=None, 
            procedure_timestamp='20120512'
        )
    ]

def test_encompassing_extract():
    XML_PATH = "ccda/ccd-encompassing.xml"
    clinical_document = read_clinical_document_from_xml_path(XML_PATH)
    extract = KonzaExtract.from_clinical_document(clinical_document)
    assert extract is not None

    name_extract = extract.patient_name_extract
    assert name_extract.name_prefix is None
    assert name_extract.given_name == "Richard"
    assert name_extract.middle_names == ""
    assert name_extract.family_name == "Maur"
    assert name_extract.name_suffix == "jr"

    address_extract = extract.patient_address_extract
    assert address_extract.street1 == "1357 Amber Dr"
    assert address_extract.street2 == ""
    assert address_extract.city == "Beaverton"
    assert address_extract.state == "OR"
    assert address_extract.postal_code == "97006"

    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == ""
    assert telecom_extract.home_phone_number_id == "+1(555)-723-1544"
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id == "+1(555)-777-1234"

    demographic_extract = extract.patient_demographic_extract
    assert demographic_extract.date_of_birth == "19800801"
    assert demographic_extract.sex == "M"
    assert demographic_extract.race is None
    assert demographic_extract.ethnicity is None
    assert demographic_extract.language == "en"
    assert demographic_extract.marital_status is None
    assert not demographic_extract.deceased

    assert extract.service_datetime == "2015072218-0500"


def test_discharge_extract():
    XML_PATH = "ccda/discharge.xml"
    clinical_document = read_clinical_document_from_xml_path(XML_PATH)
    extract = KonzaExtract.from_clinical_document(clinical_document)
    assert extract is not None

    name_extract = extract.patient_name_extract
    assert name_extract.name_prefix is None
    assert name_extract.given_name == "Isabella"
    assert name_extract.middle_names == ""
    assert name_extract.family_name == "Jones"
    assert name_extract.name_suffix is None

    address_extract = extract.patient_address_extract
    assert address_extract.street1 == "1357 Amber Drive"
    assert address_extract.street2 == ""
    assert address_extract.city == "Beaverton"
    assert address_extract.state == "OR"
    assert address_extract.postal_code == "97867"

    telecom_extract = extract.patient_telecom_extract
    assert telecom_extract.emails == ""
    assert telecom_extract.home_phone_number_id == "(816)276-6909"
    assert telecom_extract.business_phone_number_id is None
    assert telecom_extract.cell_phone_number_id is None

    demographic_extract = extract.patient_demographic_extract
    assert demographic_extract.date_of_birth == "20050501"
    assert demographic_extract.sex == "F"
    assert demographic_extract.race == "2028-9"
    assert demographic_extract.ethnicity == "2186-5"
    assert demographic_extract.language == "eng"
    assert demographic_extract.marital_status == "M"
    assert not demographic_extract.deceased

    assert extract.service_datetime == "201409091904-0500"
    assert extract.discharge_disposition_code == "01"

def xtest_discharge_summary_extract():
    XML_PATH = "ccda/discharge-summary.xml"
    clinical_document = read_clinical_document_from_xml_path(XML_PATH)
    extract = KonzaExtract.from_clinical_document(clinical_document)
    assert extract is not None

def test_multiple_payers_extract():
    XML_PATH = "ccda/deceased-2payers.xml"
    clinical_document = read_clinical_document_from_xml_path(XML_PATH)
    extract = KonzaExtract.from_clinical_document(clinical_document)
    assert extract is not None
    assert len(extract.insurance_extracts) == 2