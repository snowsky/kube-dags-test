from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr
from .common import XML_CONFIG
from typing import Optional, ClassVar


class Telecom(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    value: Optional[str] = attr(tag="value", default=None)
    use: Optional[str] = attr(tag="use", default=None)
    nullFlavor: Optional[str] = attr(tag="nullFlavor", default=None)
