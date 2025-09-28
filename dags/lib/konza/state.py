from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .template_id import TemplateId
from typing import Optional, List, ClassVar
from .code import Code


class Td(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    td: List[str] = element()


class Tbody(BaseXmlModel, tag="tbody"):
    xml_config: ClassVar = XML_CONFIG
    tr: List[Td] = element(tag="tr", default=None)


class Table(BaseXmlModel, tag="table"):
    xml_config: ClassVar = XML_CONFIG
    tbody: Tbody = element(tag="tbody", default=None)
