from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .code import Code
from typing import ClassVar


class LanguageCommunication(BaseXmlModel, tag="languageCommunication"):
    xml_config: ClassVar = XML_CONFIG
    languageCode: Code
