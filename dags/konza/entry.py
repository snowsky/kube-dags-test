from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from konza.act import Act
from konza.encounter import Encounter
from konza.observation import Observation
from konza.organizer import Organizer
from konza.procedure import Procedure
from konza.supply import Supply
from konza.substance_administration import SubstanceAdministration
from konza.service_event import ServiceEvent
from konza.organizer_component import OrganizerComponent
from typing import List, Union

RIMType = Union[
    Act,
    Encounter,
    Observation,
    Procedure,
    Organizer,
    ServiceEvent,
    SubstanceAdministration,
    Supply,
]

class Entry(BaseXmlModel, tag="entry", **PYXML_KWARGS):
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    events: List[RIMType] = element()

# Necessary due to recursive typing in ReferenceInformationModel
Act.model_rebuild()
Encounter.model_rebuild()
Observation.model_rebuild()
Procedure.model_rebuild()
Organizer.model_rebuild()
ServiceEvent.model_rebuild()
SubstanceAdministration.model_rebuild()
Supply.model_rebuild()
OrganizerComponent.model_rebuild()
