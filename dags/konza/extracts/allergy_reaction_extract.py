from pydantic_xml import BaseXmlModel, element, attr
from konza.extracts.allergy_severity_observation_extract import AllergySeverityObservationExtract 
from typing import Optional, List


class AllergyReactionExtract(BaseXmlModel):
    severity: List[AllergySeverityObservationExtract] = element()
    reaction: Optional[str] = element()
