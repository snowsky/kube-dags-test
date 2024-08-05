from pydantic_xml import BaseXmlModel, attr
from .common import PYXML_KWARGS
from typing import Optional, ForwardRef, List

class Reference(BaseXmlModel, **PYXML_KWARGS):
    value: Optional[str] = attr(default=None)
