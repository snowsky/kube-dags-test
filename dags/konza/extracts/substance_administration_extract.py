from pydantic_xml import BaseXmlModel, element, attr
from typing import List, Optional

class SubstanceAdministrationExtract(BaseXmlModel):
    normalized_code: Optional[str] = element()
    normalized_codesystem: Optional[str] = element()
    name: Optional[str] = element()
    route: Optional[str] = element()
    sig: Optional[str] = element()
    strength: Optional[str] = element()
    packaging: Optional[str] = element()
    dosage: Optional[str] = element()
