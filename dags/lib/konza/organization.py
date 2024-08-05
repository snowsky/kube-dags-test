from pydantic_xml import BaseXmlModel, attr, element
from .entity import Entity
from typing import Optional


class Organization(Entity):
    name: Optional[str] = element(tag="name", default=None)
