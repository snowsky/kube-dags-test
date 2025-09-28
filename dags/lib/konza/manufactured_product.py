from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .template_id import TemplateId
from typing import Optional, ClassVar
from .code import Code
from typing import List, ClassVar
from .rim import ReferenceInformationModel
from .manufactured_material import ManufacturedMaterial

class ManufacturedProduct(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    model_config = PYDANTIC_CONFIG
    templateId: List[TemplateId] = element(tag="templateId", default=[])
    manufacturedMaterial: Optional[ManufacturedMaterial] = element(
        tag="manufacturedMaterial", default=None
    )
