from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .section import Section
from typing import List


class StructuredBodyComponent(
    BaseXmlModel, tag="component", **PYXML_KWARGS
):
    section: List[Section]
