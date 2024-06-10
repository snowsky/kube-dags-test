from pydantic import BaseModel
from datetime import date
from typing import Annotated, Optional
from pydantic.functional_validators import AfterValidator, BeforeValidator
import re


def check_alpha(value: str):
    alpha_regex = "^[a-z A-Z]+$"
    assert re.match(alpha_regex, value)
    return value


def check_char(value: str):
    assert len(value) == 1
    return value


def truncate_int(value):
    return int(value)


alphaString = Annotated[str, AfterValidator(check_alpha)]
char = Annotated[str, AfterValidator(check_char)]
trunc_int = Annotated[int, BeforeValidator(truncate_int)]


class PopulationDefinitionEntry(BaseModel):
    mpi: Optional[int] = None
    patient_first_name: alphaString
    patient_last_name: alphaString
    date_of_birth: date  # pydantic only accepts dates in YYYY-MM-DD fmt.
    gender: char
    residential_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[int] = None
    insurance_id: Optional[int] = None
    custom_text: Optional[str] = None
    attributed_provider_first_name: Optional[str] = None
    attributed_provider_last_name: Optional[str] = None
    attributed_provider_npi: trunc_int

    def get_formatted_date(self):
        return self.date_of_birth.strftime("%m/%d/%Y")
