from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .template_id import TemplateId
from .effective_time import EffectiveTime
from .code import Code
from typing import Optional, ClassVar


class EncompassingEncounter(BaseXmlModel, tag="encompassingEncounter"):
    xml_config: ClassVar = XML_CONFIG
    id: TemplateId = element(tag="id")
    effectiveTime: EffectiveTime = element()
    dischargeDispositionCode: Optional[Code] = element(
        tag="dischargeDispositionCode", default=None
    )
