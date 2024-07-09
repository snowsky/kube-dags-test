from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.name import Name
from konza.birth_time import BirthTime
from konza.deceased_ind import DeceasedInd
from konza.code import Code
from konza.language_communication import LanguageCommunication
from typing import Optional


class Person(BaseXmlModel, **PYXML_KWARGS):
    name: Name
