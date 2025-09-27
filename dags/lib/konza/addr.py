from __future__ import annotations

from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from typing import List, Optional, ClassVar


# Note: XML_CONFIG is used directly in xml_config ClassVar


class Addr(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    streetAddressLine: List[str] = element(tag="streetAddressLine", default=[])
    city: Optional[str] = element(default=None)
    state: Optional[str] = element(default=None)
    postalCode: Optional[str] = element(default=None)
