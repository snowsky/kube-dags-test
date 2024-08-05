from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .effective_time import EffectiveTime
from .associated_entity import AssociatedEntity
from .participant_role import ParticipantRole

class Participant(BaseXmlModel, **PYXML_KWARGS):
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    time: Optional[EffectiveTime] = element(tag="time", default=None)
    associatedEntity: Optional[AssociatedEntity] = element(tag="associatedEntity", default=None)
    participantRole: Optional[ParticipantRole] = element(tag="participantRole", default=None)
    associatedEntity: Optional[AssociatedEntity] = element(tag="associatedEntity", default=None)
