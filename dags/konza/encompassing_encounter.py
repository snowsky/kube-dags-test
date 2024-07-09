from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from konza.effective_time import EffectiveTime
from konza.code import Code
from typing import Optional


class EncompassingEncounter(
    BaseXmlModel, tag="encompassingEncounter", **PYXML_KWARGS
):
    id: TemplateId = element(tag="id")
    effectiveTime: EffectiveTime = element()
    dischargeDispositionCode: Optional[Code] = element(
        tag="dischargeDispositionCode", default=None
    )
