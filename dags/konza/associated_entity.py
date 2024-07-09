from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.addr import Addr
from konza.telecom import Telecom
from typing import List, Optional
from konza.code import Code
from konza.person import Person


class AssociatedEntity(BaseXmlModel, tag="associatedEntity", **PYXML_KWARGS):
    code: Optional[Code] = element(default=None)
    addr: Optional[Addr] = element(default=None)
    telecom: List[Telecom] = element(default=[])
    associatedPerson: List[Person] = element(tag="associatedPerson", default=[])
