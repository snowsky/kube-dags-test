from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .encompassing_encounter import EncompassingEncounter


class ComponentOf(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    encompassingEncounter: EncompassingEncounter = element()
