from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr
from .common import XML_CONFIG
from typing import Optional, ClassVar

class Reference(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    value: Optional[str] = attr(default=None)
