from pydantic_xml import BaseXmlModel, attr
from .common import PYXML_KWARGS
from typing import Optional


class Telecom(BaseXmlModel, tag="telecom", **PYXML_KWARGS):
    value: Optional[str] = attr(tag="value", default=None)
    use: Optional[str] = attr(tag="use", default=None)
    nullFlavor: Optional[str] = attr(tag="nullFlavor", default=None)
