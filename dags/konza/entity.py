from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from konza.addr import Addr
from konza.patient import Patient
from konza.telecom import Telecom
from typing import List, Optional
from konza.code import Code


class Entity(BaseXmlModel, **PYXML_KWARGS):
    id: List[TemplateId] = element(tag="id", default=None)
    code: Optional[Code] = element(tag="code", default=None)
    addr: Optional[Addr] = element(tag="addr", default=None)
    telecom: List[Telecom] = element(tag="telecom", default=[])
    signatureCode: Optional[Code] = element(tag="signatureCode", default=None)

    def get_npi_id(self):
        for id in self.id:
            if id.is_npi():
                return id
        return None
