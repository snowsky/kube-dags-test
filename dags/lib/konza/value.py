from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List


class Value(BaseXmlModel, **PYXML_KWARGS):
    nullFlavor: Optional[str] = attr(tag="nullFlavor", default=None) 
    value: Optional[str] = attr(tag="value", default=None)
    code: Optional[str] = attr(tag="code", default=None)
    unit: Optional[str] = attr(tag="unit", default=None)
    displayName: Optional[str] = attr(tag="displayName", default=None)
    originalText: Optional[str] = attr(tag="originalText", default=None)
