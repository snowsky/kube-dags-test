from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .template_id import TemplateId
from typing import List, Optional, ClassVar
from .playing_entity import PlayingEntity


class SpecimenRole(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    id: Optional[TemplateId] = element(default=None)
    classCode: str = attr()
    specimenPlayingEntity: Optional[PlayingEntity] = element(tag="specimenPlayingEntity", default=None)
