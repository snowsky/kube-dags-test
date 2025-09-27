from pydantic_xml import BaseXmlModel, element, attr
from typing import Optional


class EncounterExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    service_datetime_start: Optional[str] = element()
    service_datetime_end: Optional[str] = element()
    encounter_code: Optional[str] = element()
    encounter_code_display_name: Optional[str] = element()
    encounter_phin_vads_code: Optional[str] = element()
    encounter_phin_vads_code_display_name: Optional[str] = element()
    discharged_time: Optional[str] = element()
    discharge_disposition_code: Optional[str] = element()
    discharge_disposition_display_name: Optional[str] = element()
    discharged_to: Optional[str] = element()
