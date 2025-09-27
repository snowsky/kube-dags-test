from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import KonzaBaseXmlModel
from .template_id import TemplateId
from .addr import Addr
from .patient import Patient
from .telecom import Telecom
from typing import List, Optional, ClassVar
from .code import Code


class Entity(KonzaBaseXmlModel):
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
