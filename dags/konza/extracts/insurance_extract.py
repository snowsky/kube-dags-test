from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class InsuranceExtract(BaseXmlModel):
    grpid: Optional[str] = element()
    eff_date: Optional[str] = element()
    exp_date: Optional[str] = element()
    plan_type: Optional[str] = element()
