from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from typing import List
from konza.rim import ReferenceInformationModel
from konza.manufactured_material import ManufacturedMaterial

class ManufacturedProduct(BaseXmlModel, tag="manufacturedProduct", **PYXML_KWARGS):
    templateId: List[TemplateId] = element(tag="templateId", default=[])
    manufacturedMaterial: Optional[ManufacturedMaterial] = element(
        tag="manufacturedMaterial", default=None
    )
