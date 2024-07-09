from pydantic_xml import BaseXmlModel, element, attr
from konza.common import PYXML_KWARGS
from konza.service_event import ServiceEvent
from typing import Optional


class DocumentationOf(BaseXmlModel, tag="documentationOf", **PYXML_KWARGS):
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    serviceEvent: ServiceEvent = element()
