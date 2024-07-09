from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class AllergySeverityObservationExtract(BaseXmlModel):
    description_sev: Optional[str] = element()
    code_sev: Optional[str] = element()
