from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .effective_time import EffectiveTime
from .associated_entity import AssociatedEntity
from .playing_entity import PlayingEntity
from .scoping_entity import ScopingEntity


class ParticipantRole(BaseXmlModel, **PYXML_KWARGS):
    templateId: Optional[TemplateId] = element(default=None)
    id: Optional[TemplateId] = element(tag="id", default=None)
    # note that PlayingEntity and PlayingDevice are the same class
    playingDevice: Optional[PlayingEntity] = element(tag="playingDevice", default=None)
    playingEntity: Optional[PlayingEntity] = element(tag="playingEntity", default=None)
    scopingEntity: Optional[ScopingEntity] = element(default=None)
