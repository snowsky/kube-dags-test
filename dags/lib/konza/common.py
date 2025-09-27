from typing import ClassVar
from pydantic import ConfigDict

# Pydantic v2 config for arbitrary types
PYDANTIC_CONFIG = ConfigDict(arbitrary_types_allowed=True)

NSMAP = {
    "": "urn:hl7-org:v3",
    "sdtc": "urn:hl7-org:sdtc",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

XML_CONFIG = {
    "ns": "",
    "nsmap": NSMAP,
    "skip_empty": True,
    "search_mode": "unordered",
}

# Custom base class for XML models with proper configuration
from pydantic_xml import BaseXmlModel
from typing import ClassVar

class KonzaBaseXmlModel(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG

def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
