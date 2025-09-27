from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from .act import Act
from .encounter import Encounter
from .observation import Observation
from .organizer import Organizer
from .procedure import Procedure
from .supply import Supply
from .substance_administration import SubstanceAdministration
from .service_event import ServiceEvent
from .organizer_component import OrganizerComponent
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

# Forward references are handled automatically by Pydantic v2
