from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from konza.entry import Entry
from konza.encounter import Encounter
from konza.act import Act
from konza.organizer import Organizer
from konza.observation import Observation
from konza.procedure import Procedure
from konza.substance_administration import SubstanceAdministration
from typing import List, Callable, Type

LOINC_HISTORY_OF_ENCOUNTERS_CODE = "46240-8"
LOINC_PAYERS_SECTION_CODE = "48768-6"
LOINC_PATIENT_CARE_TEAM_SECTION_CODE = "85847-2"
LOINC_PROBLEM_LIST_CODE = "11450-4"
LOINC_HISTORY_OF_PROCEDURES_SECTION_CODE = "47519-4"
LOINC_HISTORY_OF_MEDICATIONS_SECTION_CODE = "10160-0"
LOINC_ALLERGIES_AND_ADVERSE_REACTIONS_CODE = "48765-2"
LOINC_HISTORY_OF_IMMUNIZATIONS_CODE = "11369-6"
LOINC_VITAL_SIGNS_CODE = "8716-3"
LOINC_RELEVANT_DIAGNOSTIC_TESTS_CODE = "30954-2"
LOINC_CONSULT_NOTE = "11488-4"

class Section(BaseXmlModel, tag="section", **PYXML_KWARGS):
    nullFlavor: Optional[str] = attr(tag="nullFlavor", default=None)
    templateId: Optional[TemplateId] = element(default=None)
    code: Code
    title: str = element(tag="title", default=None)
    text: Optional[Element] = element(default=None)
    entry: List[Entry] = element(tag="entry", default=[])

    def is_history_of_medications(self):
        return self.code.code == LOINC_HISTORY_OF_MEDICATIONS_SECTION_CODE

    def is_history_of_encounters(self):
        return self.code.code == LOINC_HISTORY_OF_ENCOUNTERS_CODE
    
    def is_payers_section(self):
        return self.code.code == LOINC_PAYERS_SECTION_CODE

    def is_patient_care_team_section(self):
        return self.code.code == LOINC_PATIENT_CARE_TEAM_SECTION_CODE
    
    def is_problem_list_section(self):
        return self.code.code == LOINC_PROBLEM_LIST_CODE

    def is_history_of_procedures_section(self):
        return self.code.code == LOINC_HISTORY_OF_PROCEDURES_SECTION_CODE

    def is_allergies_section(self):
        return self.code.code == LOINC_ALLERGIES_AND_ADVERSE_REACTIONS_CODE

    def is_history_of_immunizations(self):
        return self.code.code == LOINC_HISTORY_OF_IMMUNIZATIONS_CODE

    def is_vital_signs_section(self):
        return self.code.code == LOINC_VITAL_SIGNS_CODE
    
    def is_labs_section(self):
        return self.code.code == LOINC_RELEVANT_DIAGNOSTIC_TESTS_CODE
    
    def is_consult_note_section(self):
        return self.code.code == LOINC_CONSULT_NOTE

    def _extract_list_from_singleton_entry(
        self,
        validation_function: Callable,
        event_type: Type,
    ) -> List[Entry]:
        if not validation_function(self):
            raise ValueError(f"Section does not pass validation function {validation_function}: {self.model_dump_json()}")
        if len(self.entry) == 0:
            return []
        if len(self.entry) > 1:
            raise ValueError("Multiple entries found when a single one was expected.")
        events = [x for x in self.entry[0].events if isinstance(x, event_type)]
        if len(events) < len(self.entry[0].events):
            raise ValueError(f"Events other than {event_type} found in history of encounters.")
        return events
    
    def _extract_singleton_events_from_entries(
        self,
        validation_function: Callable,
        event_type: Type,
        other_types_allowed: bool = False
    ) -> List[Entry]:
        if not validation_function(self):
            raise ValueError(f"Section does not pass validation function: {validation_function}: {self.model_dump_json()}")
        events = []
        for entry in self.entry:
            if len(entry.events) > 1:
                raise ValueError("Multiple events found in entry when a single one was expected")
            event = entry.events[0]
            if isinstance(event, event_type):
                events.append(event)
            elif not other_types_allowed:
                raise ValueError(f"Non-{event_type} event found in entry: {type(event)}")
        return events

    def _extract_list_from_entries(
        self,
        validation_function: Callable,
        event_type: Type,
        other_types_allowed: bool = False
    ) -> List[Entry]:
        if not validation_function(self):
            raise ValueError(f"Section does not pass validation function: {validation_function}: {self.model_dump_json()}")
        events = []
        total_events = 0
        for entry in self.entry:
            total_events += len(entry.events)
            events.extend([x for x in entry.events if isinstance(x, event_type)])
        if not other_types_allowed and len(events) < total_events:
            raise ValueError(f"Events other than {event_type} found in history of encounters.")
        return events

    def get_encounter_history(self) -> List[Encounter]:
        return self._extract_singleton_events_from_entries(
            Section.is_history_of_encounters,
            event_type=Encounter,
        )
    
    def get_coverage_activity(self) -> List[Act]:
        return self._extract_list_from_entries(
            Section.is_payers_section,
            event_type=Act,
        )

    def get_patient_care_team_organizer(self) -> Optional[Organizer]:
        organizer = self._extract_list_from_singleton_entry(
            Section.is_patient_care_team_section,
            event_type=Organizer,
        )
        if len(organizer) > 1:
            raise ValueError("Multiple organizers found in patient care team section.")
        if len(organizer) == 1:
            return organizer[0]
        return None
    
    def get_problem_events(self) -> List[Act]:
        return self._extract_singleton_events_from_entries(
            validation_function=Section.is_problem_list_section,
            event_type=Act,
        ) 
    
    def get_history_of_procedures(self) -> List[Procedure]:
        return self._extract_singleton_events_from_entries(
            Section.is_history_of_procedures_section,
            event_type=Procedure,
            other_types_allowed=True,
        )
    
    def get_history_of_medications(self) -> List[SubstanceAdministration]:
        return self._extract_singleton_events_from_entries(
            Section.is_history_of_medications,
            event_type=SubstanceAdministration,
            other_types_allowed=False,
        )
    
    def get_allergies_and_adverse_reactions(self) -> List[Act]:
        return self._extract_singleton_events_from_entries(
            validation_function=Section.is_allergies_section,
            event_type=Act,
        ) 
    
    def get_history_of_immunizations(self) -> List[SubstanceAdministration]:
        return self._extract_singleton_events_from_entries(
            validation_function=Section.is_history_of_immunizations,
            event_type=SubstanceAdministration,
        ) 
    
    def get_vital_signs(self) -> List[Observation]:
        organizers = self._extract_singleton_events_from_entries(
            validation_function=Section.is_vital_signs_section,
            event_type=Organizer,
        )
        return [x.observation for organizer in organizers for x in organizer.component if x.observation is not None]

    def get_labs(self) -> List[Observation]:
        organizers = self._extract_singleton_events_from_entries(
            validation_function=Section.is_labs_section,
            event_type=Organizer,
        )
        return [x.observation for organizer in organizers for x in organizer.component if x.observation is not None]
