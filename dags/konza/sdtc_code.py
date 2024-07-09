from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.reference import Reference
from konza.text import Text
from typing import Optional, ForwardRef, List, Union
from konza.code import Code

kwargs = {**PYXML_KWARGS}
kwargs["ns"] = "sdtc"
class SDTCCode(Code, **kwargs):
    pass
