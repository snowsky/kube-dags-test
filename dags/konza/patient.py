from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.name import Name
from konza.birth_time import BirthTime
from konza.deceased_ind import DeceasedInd
from konza.code import Code
from konza.language_communication import LanguageCommunication
from typing import Optional
from konza.person import Person


class Patient(Person, tag="patient"):
    administrativeGenderCode: Code
    birthTime: BirthTime
    deceased: Optional[DeceasedInd] = element(
        tag="deceasedInd", namespace="sdtc", default=None
    )
    maritalStatusCode: Optional[Code] = element(default=None)
    raceCode: Code
    ethnicGroupCode: Code = element(tag="ethnicGroupCode")
    languageCommunication: LanguageCommunication
