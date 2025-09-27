from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional
from ..common import PYDANTIC_CONFIG


class ProblemObservationExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    code: Optional[str] = element()
    display_name: Optional[str] = element()
    description: Optional[str] = element()
    provider_id: Optional[str] = element()
    diagnosis_timestamp: Optional[str] = element()
