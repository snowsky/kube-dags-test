from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional, List
from konza.code import Code


class Td(BaseXmlModel, tag="td", **PYXML_KWARGS):
    td: List[str] = element()


class Tbody(BaseXmlModel, tag="tbody", **PYXML_KWARGS):
    tr: List[Td] = element(tag="tr", default=None)


class Table(BaseXmlModel, tag="table", **PYXML_KWARGS):
    tbody: Tbody = element(tag="tbody", default=None)
