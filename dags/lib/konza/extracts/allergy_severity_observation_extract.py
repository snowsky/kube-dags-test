from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class AllergySeverityObservationExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    description_sev: Optional[str] = element()
    code_sev: Optional[str] = element()
