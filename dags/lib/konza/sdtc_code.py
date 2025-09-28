from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .reference import Reference
from .text import Text
from typing import Optional, List, Union, ClassVar
from .code import Code

class SDTCCode(Code):
    xml_config: ClassVar = {**XML_CONFIG, "ns": "sdtc"}
