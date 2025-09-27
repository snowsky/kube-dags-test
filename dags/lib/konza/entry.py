from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .template_id import TemplateId
from typing import Optional, ClassVar
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

class Entry(BaseXmlModel, tag="entry"):
    xml_config: ClassVar = XML_CONFIG
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    events: List[RIMType] = element()

# Note: model_rebuild() is called in __init__.py after all models are imported
