from lib.konza.structured_body_component import StructuredBodyComponent
from lib.konza.section import Section

def hl7_section(file_path: str) -> Section:
    
    with open(file_path) as f:
        xml = f.read()
        namespaced = f"""
        <component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="urn:hl7-org:v3" xmlns:voc="urn:hl7-org:v3/voc" xmlns:sdtc="urn:hl7-org:sdtc">
        {xml}
        </component>
        """
        return StructuredBodyComponent.from_xml(namespaced).section[0]


def test_discharge_billable():
    
    section = hl7_section("snippets/discharge-billable.xml")
    assert section.is_history_of_encounters()
    encounter_events = section.get_encounter_history() 
    encounter_extracts = [encounter.as_extract() for encounter in encounter_events]

def test_discharge_rehab():
    
    section = hl7_section("snippets/discharge-rehab.xml")
    assert section.is_history_of_encounters()
    encounter_events = section.get_encounter_history() 
    encounter_extracts = [encounter.as_extract() for encounter in encounter_events]

def test_payer():
    
    section = hl7_section("snippets/payer.xml")
    assert section.is_payers_section()
    acts = section.get_coverage_activity() 
    extracts = [act.as_insurance_extract() for act in acts]
    assert len(extracts) == 1
    extract = extracts[0]
    assert extract.grpid == "3e676a50-7aac-11db-9fe1-0800200c9a66"
    assert extract.eff_date is None
    assert extract.exp_date is None
    assert extract.plan_type == "C1"

def test_care_team():
    
    section = hl7_section("snippets/care-team.xml")
    assert section.is_patient_care_team_section()
    organizer = section.get_patient_care_team_organizer()
    extract = organizer.as_care_team_extract()
    assert extract.attributed_npi == '5555555555'
    assert extract.fname == "John D"
    assert extract.lname == "Smith"
    assert extract.mname == ""
    assert extract.suffix == "MD"
    assert extract.prov_type == "PCP"

def test_sig():
    
    section = hl7_section("snippets/sig.xml")
    assert section.is_history_of_medications()
    substance_administrations = section.get_history_of_medications()
    assert len(substance_administrations) == 1
    sa = substance_administrations[0]
    assert len(sa.entryRelationship) > 0 
    instr = sa.get_medication_instructions_if_any()
    assert instr is not None
    extract = substance_administrations[0].as_substance_administration_extract()
    assert extract.sig.strip() == 'some instructions'

def test_drug_mixture():
    
    section = hl7_section("snippets/mixture.xml")
    assert section.is_history_of_medications()
    substance_administrations = section.get_history_of_medications()
    assert len(substance_administrations) == 1
    
    sa = substance_administrations[0]
    extract = substance_administrations[0].as_substance_administration_extract()
    assert extract.normalized_code is None
    assert extract.normalized_codesystem == "2.16.840.1.113883.6.88"
    assert extract.name is None
    assert extract.route == "Oropharyngeal Route of Administration"
    assert extract.sig is None
    assert extract.strength is None
    assert extract.packaging is None
    assert extract.dosage is None

def test_med_instructions():
    
    section = hl7_section("snippets/med-indication-instructions.xml")
    assert section.is_history_of_medications()
    substance_administrations = section.get_history_of_medications()
    assert len(substance_administrations) == 3
    
    sa = substance_administrations[0]
    extract = substance_administrations[0].as_substance_administration_extract()
    
    assert extract.normalized_code == "197806"
    assert extract.normalized_codesystem == "2.16.840.1.113883.6.88"
    assert extract.name == "ibuprofen 600 MG Oral Tablet"
    assert extract.route is None
    assert extract.sig.strip() == ""
    assert extract.strength is None
    assert extract.packaging is None
    assert extract.dosage is None
