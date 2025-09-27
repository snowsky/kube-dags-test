from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .structured_body_component import StructuredBodyComponent
from typing import List, ClassVar


class StructuredBody(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    component: List[StructuredBodyComponent] = element(tag="component")
