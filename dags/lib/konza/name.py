from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from typing import List, Optional
from .token import Token

def concat_tokens_no_qualifiers(tokens: List[Token]) -> str:
    return " ".join([x.value for x in tokens if x.qualifier is None])

class Name(BaseXmlModel, tag="name", **PYXML_KWARGS):
    prefix: List[Token] = element(tag="prefix", default=[])
    given: List[Token] = element(tag="given", default=[])
    middle: List[Token] = element(tag="given", default=[])
    family: List[Token] = element(tag="family", default=[])
    suffix: List[Token] = element(tag="suffix", default=[])

    def get_first_name_no_qualifiers(self) -> str:
        return concat_tokens_no_qualifiers(self.given)
    
    def get_last_name_no_qualifiers(self) -> str:
        return concat_tokens_no_qualifiers(self.family)
    
    def get_middle_name_no_qualifiers(self) -> str:
        return concat_tokens_no_qualifiers(self.middle)
    
    def get_suffix_no_qualifiers(self) -> str:
        return concat_tokens_no_qualifiers(self.suffix)
