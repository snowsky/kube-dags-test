from pydantic_xml import BaseXmlModel, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS


class BirthTime(BaseXmlModel, tag="birthTime", **PYXML_KWARGS):
    dob: str = attr(name="value")
