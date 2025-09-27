from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .template_id import TemplateId
from typing import Optional, ClassVar
from .code import Code
from typing import List, ClassVar


class Value(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    nullFlavor: Optional[str] = attr(tag="nullFlavor", default=None) 
    value: Optional[str] = attr(tag="value", default=None)
    code: Optional[str] = attr(tag="code", default=None)
    unit: Optional[str] = attr(tag="unit", default=None)
    displayName: Optional[str] = attr(tag="displayName", default=None)
    originalText: Optional[str] = attr(tag="originalText", default=None)
