from typing import ClassVar

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

def coalesce(*args):
    for arg in args:
        if arg is not None:
            return arg
