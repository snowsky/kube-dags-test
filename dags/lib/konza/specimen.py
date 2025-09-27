from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .template_id import TemplateId
from typing import List, Optional, ClassVar
from .specimen_role import SpecimenRole


class Specimen(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    specimenRole: SpecimenRole = element(tag="specimenRole", default=None)
