from pydantic_xml import BaseXmlModel, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .code import Code


class LanguageCommunication(
    BaseXmlModel, tag="languageCommunication", **PYXML_KWARGS
):
    languageCode: Code
