from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr
from .common import XML_CONFIG, PYDANTIC_CONFIG
from typing import Optional, ClassVar

class Reference(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    value: Optional[str] = attr(default=None)
