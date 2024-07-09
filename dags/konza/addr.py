from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from typing import List, Optional


kwargs = {**PYXML_KWARGS}
kwargs["search_mode"] = "unordered"


class Addr(BaseXmlModel, tag="addr", **kwargs):
    streetAddressLine: List[str] = element(tag="streetAddressLine", default=[])
    city: Optional[str] = element(default=None)
    state: Optional[str] = element(default=None)
    postalCode: Optional[str] = element(default=None)
