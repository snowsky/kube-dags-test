from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .addr import Addr
from .telecom import Telecom
from typing import List, Optional
from .code import Code
from .person import Person


class AssociatedEntity(BaseXmlModel, tag="associatedEntity", **PYXML_KWARGS):
    code: Optional[Code] = element(default=None)
    addr: Optional[Addr] = element(default=None)
    telecom: List[Telecom] = element(default=[])
    associatedPerson: List[Person] = element(tag="associatedPerson", default=[])
