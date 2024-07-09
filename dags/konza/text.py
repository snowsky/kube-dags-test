from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.reference import Reference
from typing import Optional, ForwardRef, List

class Text(BaseXmlModel, **PYXML_KWARGS):
    text: Optional[str] = element(default=None)
    reference: Optional[Reference] = element(tag="reference", default=None)
