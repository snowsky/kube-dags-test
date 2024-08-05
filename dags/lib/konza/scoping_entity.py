from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .effective_time import EffectiveTime
from .associated_entity import AssociatedEntity


class ScopingEntity(BaseXmlModel, **PYXML_KWARGS):
    id: TemplateId = element(tag="id")
    desc: Optional[str] = element(default=None)
