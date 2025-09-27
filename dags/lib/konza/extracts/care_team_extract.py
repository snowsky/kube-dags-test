from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class CareTeamExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    attributed_npi: Optional[str] = element()
    fname: Optional[str] = element()
    lname: Optional[str] = element()
    mname: Optional[str] = element()
    suffix: Optional[str] = element()
    prov_type: Optional[str] = element()
