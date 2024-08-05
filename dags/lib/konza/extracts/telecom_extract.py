from pydantic_xml import BaseXmlModel
from typing import List, Optional
from ..telecom import Telecom
import collections


class TelecomExtract(BaseXmlModel):
    emails: str
    home_phone_number_id: Optional[str]
    business_phone_number_id: Optional[str]
    cell_phone_number_id: Optional[str]
    other_phone_number_id: Optional[str]

    @classmethod
    def from_telecom_list(cls, telecom: List[Telecom]):
        emails = []
        phone_numbers = collections.defaultdict(lambda: None)

        for elem in telecom:
            if elem.nullFlavor is not None:
                pass
            else:  
                if elem.value is not None and (
                    elem.value[:7] == "mailto:" or elem.value.find("@") > 0
                ):
                    emails.append(elem.value)
                elif elem.value is not None:
                    if elem.use in {"HP", "WP", "MC"}:
                        phone_numbers[elem.use] = elem.value.split("tel:")[-1] 
                    else:
                        phone_numbers["other"] = elem.value.split("tel:")[-1]
                else:
                    raise ValueError(f"Could not parse telecom: {elem}")

        clean_emails = [x[7:] if x[:7] == "mailto:" else x for x in emails]
        home_phone_number_id = phone_numbers["HP"]
        business_phone_number_id = phone_numbers["BP"]
        cell_phone_number_id = phone_numbers["MC"]
        other_phone_number_id = phone_numbers["other"]
        
        return TelecomExtract(
            emails=",".join(clean_emails),
            home_phone_number_id=home_phone_number_id,
            business_phone_number_id=business_phone_number_id,
            cell_phone_number_id=cell_phone_number_id,
            other_phone_number_id=other_phone_number_id,
        )