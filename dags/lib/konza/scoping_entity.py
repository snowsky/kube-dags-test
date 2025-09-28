from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .template_id import TemplateId
from typing import Optional, ClassVar
from .code import Code
from typing import List, ClassVar
from .effective_time import EffectiveTime
from .associated_entity import AssociatedEntity


class ScopingEntity(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    id: TemplateId = element(tag="id")
    desc: Optional[str] = element(default=None)
