from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .effective_time import EffectiveTime


class PlayingEntity(BaseXmlModel, **PYXML_KWARGS):
    code: Optional[Code] = element(default=None)
    name: Optional[str] = element(tag="name", default=None)
    classCode: Optional[str] = attr(tag="classCode", default=None)
