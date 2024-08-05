from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .rim import ReferenceInformationModel
from .manufactured_product import ManufacturedProduct


class Consumable(BaseXmlModel, tag="consumable", **PYXML_KWARGS):
    manufacturedProduct: ManufacturedProduct = element(tag="manufacturedProduct")
