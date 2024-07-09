from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class LabsObservationExtract(BaseXmlModel):
    source: Optional[str] = element()
    source_type: Optional[str] = element()
    laboratory_id: Optional[str] = element()
    obx_3_2: Optional[str] = element()
    obx_3_3: Optional[str] = element()
    obx_23_1: Optional[str] = element()
    unit: Optional[str] = element()
    range: Optional[str] = element()
    value: Optional[str] = element()
    abnormal: Optional[bool] = element()
    status: Optional[str] = element()
    time: Optional[str] = element()
    source_type: Optional[str] = element()
    hl7_code: Optional[str] = element()
    name: Optional[str] = element()
    category_id: Optional[str] = element()
    sequence: Optional[str] = element()
