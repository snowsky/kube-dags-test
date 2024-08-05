from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .structured_body import StructuredBody


class Component(BaseXmlModel, tag="component", **PYXML_KWARGS):
    structuredBody: StructuredBody = element()
