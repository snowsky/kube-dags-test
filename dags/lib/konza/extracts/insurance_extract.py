from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional
from ..common import PYDANTIC_CONFIG


class InsuranceExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    grpid: Optional[str] = element()
    eff_date: Optional[str] = element()
    exp_date: Optional[str] = element()
    plan_type: Optional[str] = element()
