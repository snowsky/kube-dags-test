from pydantic_xml import BaseXmlModel, element, attr
from typing import List
from ..extracts.problem_observation_extract import ProblemObservationExtract

class ProblemExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    observations: List[ProblemObservationExtract] = element()
