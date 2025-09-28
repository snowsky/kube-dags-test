from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .name import Name
from .birth_time import BirthTime
from .deceased_ind import DeceasedInd
from .code import Code
from .language_communication import LanguageCommunication
from typing import Optional, ClassVar


class Person(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    name: Name
