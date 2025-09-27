from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional
from ..common import PYDANTIC_CONFIG


class VitalSignsObservationExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    source: Optional[str] = element()
    code: Optional[str] = element()
    codesystem: Optional[str] = element()
    name: Optional[str] = element()
    description: Optional[str] = element()
    vital_timestamp: Optional[str] = element()
