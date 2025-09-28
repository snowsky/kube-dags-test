from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from typing import ClassVar

class DeceasedInd(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    xml_config: ClassVar = {**XML_CONFIG, "ns": "sdtc"}
    value: str = attr(name="value")
