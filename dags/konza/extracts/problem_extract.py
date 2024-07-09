from pydantic_xml import BaseXmlModel, element, attr
from typing import List
from konza.extracts.problem_observation_extract import ProblemObservationExtract

class ProblemExtract(BaseXmlModel):
    observations: List[ProblemObservationExtract] = element()
