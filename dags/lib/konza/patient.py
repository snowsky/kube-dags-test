from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .name import Name
from .birth_time import BirthTime
from .deceased_ind import DeceasedInd
from .code import Code
from .language_communication import LanguageCommunication
from typing import Optional
from .person import Person


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
