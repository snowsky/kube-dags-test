from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .reference import Reference
from .text import Text
from typing import Optional, List, Union
from .code import Code

kwargs = {**PYXML_KWARGS}
kwargs["ns"] = "sdtc"
class SDTCCode(Code, **kwargs):
    pass
