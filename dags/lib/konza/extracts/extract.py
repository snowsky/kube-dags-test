import collections
from pydantic import field_serializer
from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from ..common import XML_CONFIG
from ..clinical_document import ClinicalDocument
from typing import List, Optional, ClassVar
from ..extracts.name_extract import NameExtract
from ..extracts.address_extract import AddressExtract
from ..extracts.demographic_extract import DemographicExtract
from ..extracts.telecom_extract import TelecomExtract
from ..extracts.encounter_extract import EncounterExtract
from ..extracts.insurance_extract import InsuranceExtract
from ..extracts.care_team_extract import CareTeamExtract
from ..extracts.problem_extract import ProblemExtract
from ..extracts.problem_observation_extract import ProblemObservationExtract
from ..extracts.procedure_extract import ProcedureExtract
from ..extracts.substance_administration_extract import SubstanceAdministrationExtract
from ..extracts.allergy_extract import AllergyExtract
from ..extracts.immunization_extract import ImmunizationExtract
from ..extracts.vital_signs_observation_extract import VitalSignsObservationExtract
from ..extracts.labs_observation_extract import LabsObservationExtract

class KonzaExtractBase(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    patient_name_extract: NameExtract
    patient_address_extract: AddressExtract
    patient_demographic_extract: DemographicExtract
    patient_telecom_extract: TelecomExtract
    
    @field_serializer(
        "patient_name_extract",
        "patient_address_extract",
        "patient_demographic_extract",
        "patient_telecom_extract",
    )
    def serialize_pipe_delimited(self, field, _info):
        return "|".join(
            [str(x) if x is not None else "" for x in dict(field).values()]
        )

    def dump_pipe_delimited(self):
        return "|".join(
            [
                str(x) if x is not None else ""
                for x in self.model_dump().values()
            ]
        )
    
    @classmethod
    def from_clinical_document(cls, doc: ClinicalDocument):
        record_target = doc.recordTarget
        patient_role = record_target.patientRole
        patient = patient_role.patient

        demographic_extract = DemographicExtract.from_patient(patient)
        name_extract = NameExtract.from_name(patient.name)
        address_extract = AddressExtract.from_addr(patient_role.addr)
        telecom_extract = TelecomExtract.from_telecom_list(patient_role.telecom)

        return KonzaExtractBase(
            patient_demographic_extract=demographic_extract,
            patient_name_extract=name_extract,
            patient_address_extract=address_extract,
            patient_telecom_extract=telecom_extract,
        )

class KonzaExtract(KonzaExtractBase):
    
    service_datetime: Optional[str]
    discharge_disposition_code: Optional[str]
    
    encounter_extracts: List[EncounterExtract]
    insurance_extracts: List[InsuranceExtract]
    care_team_extract: Optional[CareTeamExtract]
    problem_extracts: List[ProblemExtract]
    procedure_extracts: List[ProcedureExtract]
    medication_extracts: List[SubstanceAdministrationExtract]
    allergy_extracts: List[AllergyExtract]
    immunization_extracts: List[ImmunizationExtract]
    vital_signs_extracts: List[VitalSignsObservationExtract] 
    labs_extracts: List[LabsObservationExtract] 
    provider_notes: Optional[str]

    @classmethod
    def from_clinical_document(cls, doc: ClinicalDocument):

        base = super().from_clinical_document(doc)

        service_datetime = None
        discharge_disposition_code = None
        if doc.componentOf is not None:
            encounter = doc.componentOf.encompassingEncounter
            service_datetime = encounter.effectiveTime.low.value
            discharge = encounter.dischargeDispositionCode
            if discharge is not None:
                discharge_disposition_code = discharge.code
  
        encounter_extracts = []
        encounter_history_section = doc.get_encounter_history_section()
        if encounter_history_section is not None:
            encounter_events = encounter_history_section.get_encounter_history() 
            encounter_extracts = [encounter.as_extract() for encounter in encounter_events]

        insurance_extracts = []
        payers_section = doc.get_payers_section()
        if payers_section is not None:
            coverage_events = payers_section.get_coverage_activity()
            insurance_extracts = [act.as_insurance_extract() for act in coverage_events]
        
        care_team_section = doc.get_patient_care_team_section()
        care_team_extract = None
        if care_team_section is not None:
            organizer = care_team_section.get_patient_care_team_organizer()
            if organizer is not None: 
                care_team_extract = organizer.as_care_team_extract()
       
        problem_extracts = []
        problems_section = doc.get_problems_section()
        if problems_section is not None:
            problem_events = problems_section.get_problem_events()
            problem_extracts = [x.as_problem_extract() for x in problem_events]
       
        procedure_extracts = []
        procedures_section = doc.get_history_of_procedures_section()
        if procedures_section is not None:
            procedures = procedures_section.get_history_of_procedures()
            procedure_extracts = [x.as_procedure_extract() for x in procedures]
       
        medication_extracts = []
        history_of_medications_section = doc.get_history_of_medications_section()
        if history_of_medications_section is not None:
            objects = history_of_medications_section.get_history_of_medications()
            medication_extracts = [x.as_substance_administration_extract() for x in objects]
        
        allergy_extracts = []
        allergies_section = doc.get_allergies_section()
        if allergies_section is not None:
            allergy_events = allergies_section.get_allergies_and_adverse_reactions()
            allergy_extracts = [x.as_allergy_extract() for x in allergy_events]

        immunization_extracts = []
        immunizations_section = doc.get_history_of_immunizations_section()
        if immunizations_section is not None:
            immunization_events = immunizations_section.get_history_of_immunizations()
            immunization_extracts = [x.as_immunization_extract() for x in immunization_events]
        
        vital_signs_extracts = []
        vital_signs_section = doc.get_vital_signs_section()
        if vital_signs_section is not None:
            vital_signs_obs = vital_signs_section.get_vital_signs()
            vital_signs_obs_extracts = [
                x.as_vital_signs_observation_extract() for x in vital_signs_obs
            ]
        
        labs_extracts = []
        labs_section = doc.get_labs_section()
        if labs_section is not None:
            labs_obs = labs_section.get_labs()
            labs_obs_extracts = [
                x.as_labs_observation_extract() for x in labs_obs
            ]
            
        consult_note = doc.get_consult_note_section()

        return KonzaExtract(
            patient_name_extract=base.patient_name_extract,
            patient_address_extract=base.patient_address_extract,
            patient_telecom_extract=base.patient_telecom_extract,
            patient_demographic_extract=base.patient_demographic_extract,
            service_datetime=service_datetime,
            discharge_disposition_code=discharge_disposition_code,
            encounter_extracts=encounter_extracts,
            insurance_extracts=insurance_extracts,
            care_team_extract=care_team_extract,
            problem_extracts=problem_extracts,
            procedure_extracts=procedure_extracts,
            medication_extracts=medication_extracts,
            allergy_extracts=allergy_extracts,
            immunization_extracts=immunization_extracts,
            vital_signs_extracts=vital_signs_obs_extracts,
            labs_extracts=labs_extracts,
            provider_notes=consult_note.text.text if consult_note is not None else None,
        )
