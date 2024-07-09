from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.encompassing_encounter import EncompassingEncounter


class ComponentOf(BaseXmlModel, tag="componentOf", **PYXML_KWARGS):
    encompassingEncounter: EncompassingEncounter = element()
