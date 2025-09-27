from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from pydantic import create_model
from .common import XML_CONFIG, PYDANTIC_CONFIG
from .entity import Entity
from .organization import Organization
from .person import Person
from .template_id import TemplateId
from typing import Optional, ClassVar

def assigned_entity_model(
    model_name: str,
    represented_organization_field_name: str = "representedOrganization",
    **kwargs
):
    
    kwargs[represented_organization_field_name] = (
        Optional[Organization], element(tag=represented_organization_field_name, default=None)
    ) 
    model = create_model(
        model_name,
        assignedPerson=(Optional[Person], element(tag="assignedPerson", default=None)),
        __base__=Entity,
        __config__=PYDANTIC_CONFIG,
        **kwargs
    )

    # Add xml_config for Pydantic v2
    model.xml_config = XML_CONFIG
    return model

