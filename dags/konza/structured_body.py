from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.structured_body_component import StructuredBodyComponent
from typing import List


class StructuredBody(BaseXmlModel, tag="structuredBody", **PYXML_KWARGS):
    component: List[StructuredBodyComponent] = element(tag="component")
