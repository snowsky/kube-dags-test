from pydantic_xml import BaseXmlModel, element
from typing import List, Optional
from ..addr import Addr


class AddressExtract(BaseXmlModel):
    street1: str
    street2: str
    city: Optional[str] = element(default=None)
    state: Optional[str] = element(default=None)
    postal_code: Optional[str] = element(default=None)

    @classmethod
    def from_addr(cls, address: Addr):
        street_address = address.streetAddressLine
        return AddressExtract(
            street1=street_address[0] if len(street_address) > 0 else "",
            street2=street_address[1] if len(street_address) > 1 else "",
            city=address.city,
            state=address.state,
            postal_code=address.postalCode,
        )
