from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class ProblemObservationExtract(BaseXmlModel):
    code: Optional[str] = element()
    display_name: Optional[str] = element()
    description: Optional[str] = element()
    provider_id: Optional[str] = element()
    diagnosis_timestamp: Optional[str] = element()
