from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .act import Act
from .observation import Observation
from typing import Optional, ClassVar


class OrganizerComponent(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    act: Optional[Act] = element(default=None)
    observation: Optional[Observation] = element(default=None)
