from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.structured_body import StructuredBody


class Component(BaseXmlModel, tag="component", **PYXML_KWARGS):
    structuredBody: StructuredBody = element()
