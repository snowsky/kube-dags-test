from konza.entity_model import entity_model
from konza.sdtc_code import SDTCCode
from typing import Optional
from pydantic_xml import element

Performer = entity_model(
    "Performer",
    functionCode=(Optional[SDTCCode], element(tag="functionCode", namespace="sdtc", default=None))
)
