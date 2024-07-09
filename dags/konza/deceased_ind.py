from pydantic_xml import BaseXmlModel, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS

kwargs = {**PYXML_KWARGS}
kwargs["ns"] = "sdtc"


class DeceasedInd(BaseXmlModel, tag="deceasedInd", **kwargs):
    value: str = attr(name="value")
