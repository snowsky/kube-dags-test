from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .service_event import ServiceEvent
from typing import Optional, ClassVar


class DocumentationOf(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    serviceEvent: ServiceEvent = element()
