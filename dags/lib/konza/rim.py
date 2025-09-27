from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import NSMAP, XML_CONFIG
from .template_id import TemplateId
from typing import Optional
from .code import Code
from .author import Author
from .effective_time import EffectiveTime
from typing import List, Callable
from .participant import Participant
from .specimen import Specimen
from .performer import Performer
from .value import Value
from pydantic import ConfigDict
from typing import ClassVar


class ReferenceInformationModel(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    templateId: List[TemplateId] = element(default=[])
    id: Optional[TemplateId] = element(tag="id", default=None)
    
    code: Optional[Code] = element(tag="code", default=None)
    classCode: str = attr(tag="classCode") 
    moodCode: Optional[str] = attr(tag="moodCode", default=None) 
    methodCode: Optional[Code] = element(default=None)
    statusCode: Optional[Code] = element(tag="statusCode", default=None)
    siteCode: Optional[Code] = element(default=None)
    
    effectiveTime: Optional[EffectiveTime] = element(tag="effectiveTime", default=None)
    
    value: Optional[Value] = element(tag="value", default=None)
    entryRelationship: List['Entry'] = element(tag="entryRelationship", default=[])
    
    author: Optional[Author] = element(tag="author", default=None)
    participant: List[Participant] = element(tag="participant", default=[])
    performer: List[Performer] = element(default=[])
    
    text: Optional[str] = element(default=None)
    specimen: List[Specimen] = element(default=[])

    def get_unique_participant_by_type_code(self, type_code):
        dsts = [
            x for x in self.participant 
            if x.typeCode == type_code
        ]
        if len(dsts) > 1:
            raise ValueError(f"Multiple {type_code} participants found.")
        elif len(dsts) == 0:
            return None
        return dsts[0]

    def get_dst_participant(self) -> Optional[Participant]:
        return self.get_unique_participant_by_type_code("DST")
    
    def get_cov_participant(self) -> Optional[Participant]:
        return self.get_unique_participant_by_type_code("COV")

    def check_template_ids(self, validation_fn: Callable):
        for template_id in self.templateId:
            is_valid = validation_fn(template_id)
            if is_valid:
                return True
            else:
                import logging
                logging.error((template_id, validation_fn, is_valid))
        return False

# Note: model_rebuild() is called in __init__.py after all models are imported
