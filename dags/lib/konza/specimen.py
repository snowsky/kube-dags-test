from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import List, Optional
from .specimen_role import SpecimenRole


class Specimen(BaseXmlModel, tag="specimenRole", **PYXML_KWARGS):
    specimenRole: SpecimenRole = element(tag="specimenRole", default=None)
