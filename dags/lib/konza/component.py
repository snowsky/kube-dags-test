from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .structured_body import StructuredBody


class Component(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    structuredBody: StructuredBody = element()
