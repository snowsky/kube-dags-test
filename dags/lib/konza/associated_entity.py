from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .addr import Addr
from .telecom import Telecom
from typing import List, Optional, ClassVar
from .code import Code
from .person import Person


class AssociatedEntity(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    code: Optional[Code] = element(default=None)
    addr: Optional[Addr] = element(default=None)
    telecom: List[Telecom] = element(default=[])
    associatedPerson: List[Person] = element(tag="associatedPerson", default=[])
