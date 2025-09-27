from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG


class BirthTime(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    dob: str = attr(name="value")
