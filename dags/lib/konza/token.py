from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from typing import List, Optional, ClassVar


class Token(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    value: Optional[str] = None #= attr(default=None)
    qualifier: Optional[str] = attr(tag="qualifier", default=None)
