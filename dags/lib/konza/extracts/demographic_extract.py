from pydantic_xml import BaseXmlModel
from typing import List, Optional
from ..patient import Patient
import collections
from typing import Optional


class DemographicExtract(BaseXmlModel):
    model_config = PYDANTIC_CONFIG
    date_of_birth: str
    sex: Optional[str]
    race: Optional[str]
    ethnicity: Optional[str]
    language: Optional[str]
    marital_status: Optional[str]
    deceased: bool

    @classmethod
    def from_patient(cls, patient: Patient):
        date_of_birth = patient.birthTime.dob
        sex = patient.administrativeGenderCode.code
        race = patient.raceCode.code
        ethnicity = patient.ethnicGroupCode.code
        language = patient.languageCommunication.languageCode.code
        marital_status = (
            patient.maritalStatusCode.code
            if patient.maritalStatusCode is not None
            else None
        )
        deceased = (
            patient.deceased is not None and patient.deceased.value == "true"
        )

        return DemographicExtract(
            date_of_birth=date_of_birth,
            sex=sex,
            race=race,
            ethnicity=ethnicity,
            language=language,
            marital_status=marital_status,
            deceased=deceased,
        )
