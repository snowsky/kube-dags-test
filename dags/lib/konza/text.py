from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .reference import Reference
from typing import Optional, ClassVar

class Text(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    text: Optional[str] = element(default=None)
    reference: Optional[Reference] = element(tag="reference", default=None)
