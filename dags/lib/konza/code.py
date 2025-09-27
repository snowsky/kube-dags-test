from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .reference import Reference
from .text import Text
from typing import Optional, List, Union, ClassVar

PHIN_VADS_CODE_SYSTEM = "2.16.840.1.113883.5.4"
LOINC_STANDARD_CODE_SYSTEM = "2.16.840.1.113883.6.1"
MEDICATION_INSTRUCTIONS_LOINC_CODE = "76662-6"


class Code(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    nullFlavor: Optional[str] = attr(default=None)
    code: Optional[str] = attr(name="code", default=None)
    codeSystem: Optional[str] = attr(name="codeSystem", default=None)
    displayName: Optional[str] = attr(name="displayName", default=None)
    codeSystemName: Optional[str] = attr(name="codeSystemName", default=None)
    translation: List['Code'] = element(tag="translation", default=[])
    originalText: Optional[Text] = element(name="originalText", default=None)

    def is_loinc_standard_code(self):
        return self.codeSystem == LOINC_STANDARD_CODE_SYSTEM 

    def is_medication_instructions(self):
        return self.code == MEDICATION_INSTRUCTIONS_LOINC_CODE 

    def is_phin_vads_code(self):
        return self.codeSystem == PHIN_VADS_CODE_SYSTEM

    def best_effort_phin_vads_code(self) -> Optional['Code']:
        
        if self.is_phin_vads_code():
            return self
        codes = [x for x in self.translation if x.is_phin_vads_code()]
        if len(codes) > 1:
            raise ValueError("Multiple PHIN VADS codes found.")
        if len(codes) == 1:
            return codes[0]
        return None

# Note: model_rebuild() is called in __init__.py after all models are imported
