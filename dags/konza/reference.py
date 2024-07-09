from pydantic_xml import BaseXmlModel, attr
from konza.common import PYXML_KWARGS
from typing import Optional, ForwardRef, List

class Reference(BaseXmlModel, **PYXML_KWARGS):
    value: Optional[str] = attr(default=None)
