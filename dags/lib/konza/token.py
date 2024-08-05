from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from typing import List, Optional


class Token(BaseXmlModel, **PYXML_KWARGS):
    value: Optional[str] = None #= attr(default=None)
    qualifier: Optional[str] = attr(tag="qualifier", default=None)
