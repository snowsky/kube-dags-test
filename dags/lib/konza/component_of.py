from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .encompassing_encounter import EncompassingEncounter


class ComponentOf(BaseXmlModel, tag="componentOf", **PYXML_KWARGS):
    encompassingEncounter: EncompassingEncounter = element()
