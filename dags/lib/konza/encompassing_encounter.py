from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from .effective_time import EffectiveTime
from .code import Code
from typing import Optional


class EncompassingEncounter(
    BaseXmlModel, tag="encompassingEncounter", **PYXML_KWARGS
):
    id: TemplateId = element(tag="id")
    effectiveTime: EffectiveTime = element()
    dischargeDispositionCode: Optional[Code] = element(
        tag="dischargeDispositionCode", default=None
    )
