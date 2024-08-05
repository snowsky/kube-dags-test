from pydantic_xml import BaseXmlModel, attr, element
from pydantic import create_model
from .common import PYXML_KWARGS
from .entity import Entity
from .organization import Organization
from .person import Person
from .template_id import TemplateId
from typing import Optional

def assigned_entity_model(
    model_name: str,
    represented_organization_field_name: str = "representedOrganization",
    **kwargs
):
    
    kwargs[represented_organization_field_name] = (
        Optional[Organization], element(tag=represented_organization_field_name, default=None)
    ) 
    return create_model(
        model_name, 
        assignedPerson=(Optional[Person], element(tag="assignedPerson", default=None)),
        __cls_kwargs__=PYXML_KWARGS,
        __base__=Entity,
        **kwargs
    )

