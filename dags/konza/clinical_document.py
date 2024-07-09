from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.component import Component
from konza.component_of import ComponentOf
from konza.record_target import RecordTarget
from typing import List, Optional, Callable
from konza.code import Code
from konza.template_id import TemplateId
from konza.text import Text
from konza.effective_time import EffectiveTime
from konza.value import Value
from konza.author import Author
from konza.data_enterer import DataEnterer
from konza.informant import Informant
from konza.custodian import Custodian
from konza.legal_authenticator import LegalAuthenticator
from konza.authenticator import Authenticator
from konza.participant import Participant
from konza.documentation_of import DocumentationOf
from konza.section import Section


class ClinicalDocumentBase(BaseXmlModel, tag="ClinicalDocument", **PYXML_KWARGS):
    realmCode: Optional[Code] = element(tag="realmCode", default=None)
    typeId: Optional[TemplateId] = element(tag="typeId", default=None)
    templateId: Optional[TemplateId] = element(tag="templateId", default=None)
    id: TemplateId = element(tag="id")
    code: Optional[Code] = element(tag="code", default=None)
    title: Optional[str] = element(tag="title", default=None)
    effectiveTime: Optional[EffectiveTime] = element(tag="effectiveTime", default=None)
    confidentialityCode: Optional[Code] = element(tag="confidentialityCode", default=None)
    languageCode: Optional[Code] = element(tag="languageCode", default=None)
    setId: Optional[TemplateId] = element(tag="setId", default=None)
    versionNumber: Optional[Value] = element(tag="versionNumber", default=None)
    recordTarget: RecordTarget

class ClinicalDocument(ClinicalDocumentBase):
    
    author: List[Author] = element(tag="author")
    dataEnterer: List[DataEnterer] = element(tag="dataEnterer", default=[])
    informant: List[Informant] = element(tag="informant", default=[])
    custodian: List[Custodian] = element(tag="custodian", default=[])
    legalAuthenticator: List[LegalAuthenticator] = element(tag="legalAuthenticator", default=[])
    authenticator: List[Authenticator] = element(tag="authenticator", default=[])
    participant: List[Participant] = element(tag="participant", default=[])

    documentationOf: Optional[DocumentationOf] = element(tag="documentationOf", default=None)
    componentOf: Optional[ComponentOf] = element(default=None)
    component: List[Component] = element(tag="component")
            
    def get_flattened_structural_body_sections(self) -> List[Section]:
        return [
            section
            for component in self.component
            for structured_body_component in component.structuredBody.component
            for section in structured_body_component.section 
        ]

    def get_singleton_section(
        self,
        section_type: str,
        validation_fn: Callable,
    ) -> Optional[Section]:
        sections = [
            section
            for section in self.get_flattened_structural_body_sections()
            if validation_fn(section)
        ]
        if len(sections) == 0:
            return None
        if len(sections) > 1:
            raise ValueError(f"Multiple sections of type {section_type} found when at most one expected.")

        return sections[0]

    def get_encounter_history_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="encounter history",
            validation_fn=Section.is_history_of_encounters,
        )

    def get_payers_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="payers",
            validation_fn=Section.is_payers_section,
        )

    def get_patient_care_team_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="patient care team",
            validation_fn=Section.is_patient_care_team_section,
        )
        
    def get_problems_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="problems",
            validation_fn=Section.is_problem_list_section,
        )
    
    def get_history_of_procedures_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="history of procedures",
            validation_fn=Section.is_history_of_procedures_section,
        )
    
    def get_history_of_medications_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="history of medications",
            validation_fn=Section.is_history_of_medications,
        )
    
    def get_allergies_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="allergies",
            validation_fn=Section.is_allergies_section,
        )
    
    def get_history_of_immunizations_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="history of immunizations",
            validation_fn=Section.is_history_of_immunizations,
        )

    def get_vital_signs_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="vital signs",
            validation_fn=Section.is_vital_signs_section,
        )
    
    def get_labs_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="labs",
            validation_fn=Section.is_labs_section,
        )
    
    def get_consult_note_section(self) -> Optional[Section]:
        return self.get_singleton_section(
            section_type="consult note",
            validation_fn=Section.is_consult_note_section,
        )
