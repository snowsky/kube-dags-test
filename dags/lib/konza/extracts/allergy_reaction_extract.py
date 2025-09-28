from pydantic_xml import BaseXmlModel, element, attr
from ..extracts.allergy_severity_observation_extract import AllergySeverityObservationExtract 
from typing import Optional, List
from ..common import PYDANTIC_CONFIG


class AllergyReactionExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    severity: List[AllergySeverityObservationExtract] = element()
    reaction: Optional[str] = element()
