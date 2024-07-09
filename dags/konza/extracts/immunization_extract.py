from pydantic_xml import BaseXmlModel, element, attr
from typing import List, Optional

class ImmunizationExtract(BaseXmlModel):
    source: Optional[str] = element()
    vault_based_id: Optional[str] = element()
    performing_provider_id: Optional[str] = element()
    series_number: Optional[str] = element()
    administered_date: Optional[str] = element()
    administered_code_id: Optional[str] = element()
    event_timestamp: Optional[str] = element()
    immunization_id: Optional[str] = element()
    vaccine_id: Optional[str] = element()
    vaccine_manufacturer_id: Optional[str] = element()
    manufacturer_text: Optional[str] = element()
    lot_number: Optional[str] = element()
    vaccination_status_id: Optional[str] = element()
    dosage: Optional[str] = element()
    measure_unit_id: Optional[str] = element()
    code: Optional[str] = element()
    display: Optional[str] = element()
    description: Optional[str] = element()
    short_name: Optional[str] = element()
    long_name: Optional[str] = element()

