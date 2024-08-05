from pydantic_xml import BaseXmlModel, element, attr
from .common import PYXML_KWARGS
from .service_event import ServiceEvent
from typing import Optional


class DocumentationOf(BaseXmlModel, tag="documentationOf", **PYXML_KWARGS):
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    serviceEvent: ServiceEvent = element()
