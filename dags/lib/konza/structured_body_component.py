from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .section import Section
from typing import List, ClassVar


class StructuredBodyComponent(BaseXmlModel, tag="component"):
    xml_config: ClassVar = XML_CONFIG
    section: List[Section]
