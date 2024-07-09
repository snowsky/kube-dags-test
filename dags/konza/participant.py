from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from typing import List
from konza.effective_time import EffectiveTime
from konza.associated_entity import AssociatedEntity
from konza.participant_role import ParticipantRole

class Participant(BaseXmlModel, **PYXML_KWARGS):
    typeCode: Optional[str] = attr(tag="typeCode", default=None)
    time: Optional[EffectiveTime] = element(tag="time", default=None)
    associatedEntity: Optional[AssociatedEntity] = element(tag="associatedEntity", default=None)
    participantRole: Optional[ParticipantRole] = element(tag="participantRole", default=None)
    associatedEntity: Optional[AssociatedEntity] = element(tag="associatedEntity", default=None)
