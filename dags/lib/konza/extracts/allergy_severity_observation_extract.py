from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional
from ..common import PYDANTIC_CONFIG


class AllergySeverityObservationExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    description_sev: Optional[str] = element()
    code_sev: Optional[str] = element()
