from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional, List
from ..extracts.allergy_reaction_extract import AllergyReactionExtract
from ..common import PYDANTIC_CONFIG


class AllergyObservationExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    description: Optional[str] = element()
    code: Optional[str] = element()
    description_type: Optional[str] = element()
    code_type: Optional[str] = element()
    reaction: List[AllergyReactionExtract] = element()
    reported_date: Optional[str] = element()
    onset_date: Optional[str] = element()
    onset_date_text: Optional[str] = element()
    code_relationship: Optional[str] = element()
    description_relationship: Optional[str] = element()
    code_status: Optional[str] = element()
    description_status: Optional[str] = element()
    status_reporter: Optional[str] = element()
    reporter_organization: Optional[str] = element()
    status_date: Optional[str] = element()
