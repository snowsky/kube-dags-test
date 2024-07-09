from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from typing import List
from konza.effective_time import EffectiveTime
from konza.associated_entity import AssociatedEntity


class ScopingEntity(BaseXmlModel, **PYXML_KWARGS):
    id: TemplateId = element(tag="id")
    desc: Optional[str] = element(default=None)
