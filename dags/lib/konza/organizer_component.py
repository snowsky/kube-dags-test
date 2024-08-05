from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .act import Act
from .observation import Observation
from typing import Optional


class OrganizerComponent(BaseXmlModel, tag="component", **PYXML_KWARGS):
    act: Optional[Act] = element(default=None)
    observation: Optional[Observation] = element(default=None)
