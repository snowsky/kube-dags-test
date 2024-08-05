from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .value import Value


class Quantity(Value):
    unit: Optional[str] = attr(tag="unit", default=None)
    
