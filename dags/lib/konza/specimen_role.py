from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import List, Optional
from .playing_entity import PlayingEntity


class SpecimenRole(BaseXmlModel, tag="specimenRole", **PYXML_KWARGS):
    id: Optional[TemplateId] = element(default=None)
    classCode: str = attr()
    specimenPlayingEntity: Optional[PlayingEntity] = element(tag="specimenPlayingEntity", default=None)
