from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from typing import List
from konza.rim import ReferenceInformationModel
from konza.manufactured_product import ManufacturedProduct


class Consumable(BaseXmlModel, tag="consumable", **PYXML_KWARGS):
    manufacturedProduct: ManufacturedProduct = element(tag="manufacturedProduct")
