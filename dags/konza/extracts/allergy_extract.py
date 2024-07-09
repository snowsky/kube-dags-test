from pydantic_xml import BaseXmlModel, element, attr
from typing import List
from konza.extracts.allergy_observation_extract import AllergyObservationExtract

class AllergyExtract(BaseXmlModel):
    observations: List[AllergyObservationExtract] = element()
