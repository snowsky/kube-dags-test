from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .name import Name
from .birth_time import BirthTime
from .deceased_ind import DeceasedInd
from .code import Code
from .language_communication import LanguageCommunication
from typing import Optional


class Person(BaseXmlModel, **PYXML_KWARGS):
    name: Name
