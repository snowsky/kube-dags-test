from pydantic_xml import BaseXmlModel, element, attr
from typing import List, Optional
from konza.extracts.problem_observation_extract import ProblemObservationExtract

class ProcedureExtract(BaseXmlModel):
    code: Optional[str] = element()
    display_name: Optional[str] = element()
    provider_id: Optional[str] = element()
    original_text: Optional[str] = element()
    procedure_timestamp: Optional[str] = element()
