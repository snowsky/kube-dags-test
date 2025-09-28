from pydantic_xml import BaseXmlModel, element, attr
from typing import List
from ..extracts.allergy_observation_extract import AllergyObservationExtract
from ..common import PYDANTIC_CONFIG

class AllergyExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    observations: List[AllergyObservationExtract] = element()
