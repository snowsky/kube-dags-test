from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .template_id import TemplateId
from typing import Optional, ClassVar
from .code import Code
from typing import List, ClassVar
import typing
from .common import coalesce
from .rim import ReferenceInformationModel
from .extracts.problem_observation_extract import ProblemObservationExtract
from .extracts.vital_signs_observation_extract import VitalSignsObservationExtract
from .extracts.labs_observation_extract import LabsObservationExtract
from .extracts.allergy_observation_extract import AllergyObservationExtract
from .extracts.allergy_reaction_extract import AllergyReactionExtract
from .extracts.allergy_severity_observation_extract import AllergySeverityObservationExtract

class Observation(ReferenceInformationModel, tag="observation"):

    interpretationCode: List[Code] = element(tag='interpretationCode', default=[])

    def is_problem_observation(self) -> bool:
        return self.check_template_ids(TemplateId.is_problem_observation)

    def get_date_of_diagnosis_act_if_any(self) -> Optional['Act']:
        from .act import Act
        if not self.is_problem_observation():
            raise ValueError("Observation is not problem observation")
        for entry in self.entryRelationship:
            for event in entry.events:
                if isinstance(event, Act) and event.is_date_of_diagnosis_act():
                    return event
        return None

    def as_problem_observation_extract(self) -> ProblemObservationExtract:
        if not self.is_problem_observation():
            raise ValueError("Observation is not problem observation")
        
        code = None
        display_name = None
        provider_id = None
        description = None
        diagnosis_timestamp = None

        if self.value is not None:
            code = self.value.code
            display_name = self.value.displayName
            if self.value.originalText is not None:
                description = self.value.originalText.text
        if self.author is not None:
            if self.author.assignedAuthor is not None:
                provider_id = self.author.assignedAuthor.get_npi_id().extension
            if self.author.time is not None:
                diagnosis_timestamp = self.author.time.get_best_effort_interval_start()
        if diagnosis_timestamp is None:
            date_of_diagnosis_act = self.get_date_of_diagnosis_act_if_any()
            if date_of_diagnosis_act is not None and date_of_diagnosis_act.time is not None:
                diagnosis_timestamp = date_of_diagnosis_act.time.get_best_effort_interval_start()

        return ProblemObservationExtract(
            code=code,
            provider_id=provider_id,
            display_name=display_name,
            description=description,
            diagnosis_timestamp=diagnosis_timestamp,
        )

    def as_allergy_severity_observation_extract(self) -> AllergySeverityObservationExtract:
        if self.value is None:
            raise ValueError("Allergy severity observation must have value.")
        
        code_sev = self.value.code
        description_sev = self.value.displayName

        return AllergySeverityObservationExtract(
            code_sev=code_sev,
            description_sev=description_sev,
        )

    def as_allergy_reaction_extract(self) -> AllergyReactionExtract:
        
        severity_obs = [x for x in self.entryRelationship if x.events[0].code.code == "SEV"]
        severity = [x.as_allergy_severity_observation_extract() for entry in severity_obs for x in entry.events]
        return AllergyReactionExtract(
            reaction=self.value.displayName,
            severity=severity,
        )

    def as_allergy_observation_extract(self) -> AllergyObservationExtract:
   
        if len(self.participant) == 0:
            raise ValueError("No participants found in allergy observation.")

        if len(self.participant) > 1:
            raise ValueError("Multiple participants found in allergy observation.")

        participant = self.participant[0]
        if participant.participantRole is None or participant.participantRole.playingEntity is None:
            raise ValueError("participant.participantRole must have playingEntity in allergy observation.")
        playing_entity = participant.participantRole.playingEntity
        playing_entity_code = playing_entity.code
       
        if playing_entity_code.originalText is not None:
            description = (
                coalesce(playing_entity_code.displayName, "") + 
                "\n" + 
                coalesce(playing_entity_code.originalText.text, "")
            )
        else:
            description = playing_entity_code.displayName
        code = playing_entity_code.code

        if self.value is None:
            raise ValueError("observation value cannot be None for Allergy observation.")
        code_type = self.value.code
        description_type = self.value.displayName

        
        mfst_entries = [entry for entry in self.entryRelationship if entry.typeCode == "MFST"]
        reactions = [x.as_allergy_reaction_extract() for entry in mfst_entries for x in entry.events]
        if self.author is not None:
            reported_date = self.author.time.get_best_effort_interval_start()
        else:
            reported_date = None
        onset_date = self.effectiveTime.get_best_effort_interval_start()

        return AllergyObservationExtract(
            description=description,
            code=code,
            description_type=description_type,
            code_type=code_type,
            reaction=reactions,
            reported_date=reported_date,
            onset_date=onset_date,
            #TODO: these fields need to be filled in:
            onset_date_text=None,
            description_relationship=None,
            code_relationship=None,
            description_status=None,
            code_status=None,
            status_reporter=None,
            reporter_organization=None,
            status_date=None,
        )
    def as_vital_signs_observation_extract(self) -> VitalSignsObservationExtract:
        
        return VitalSignsObservationExtract(
            source=None,
            code=self.code.code,
            codesystem=self.code.codeSystem,
            name=self.code.displayName,
            description=None,
            vital_timestamp=self.effectiveTime.value,
        )
    
    def as_labs_observation_extract(self) -> LabsObservationExtract:
     
        abnormal = False
        for code in self.interpretationCode:
            if code.code in {'A', 'LL', 'L', 'H', 'HH'}:
                abnormal = True
                break

        return LabsObservationExtract(
            source=None,
            source_type=None,
            laboratory_id=None,
            obx_3_2=None,
            obx_3_3=self.code.code if self.code.codeSystem == "2.16.840.1.113883.6.1" else None,
            obx_23_1=None,
            unit=self.value.unit,
            range=None,
            value=self.value.value,
            abnormal=abnormal,
            status=None,
            time=self.effectiveTime.value,
            hl7_code=None,
            name=None,
            category_id=None,
            sequence=None,
        )
