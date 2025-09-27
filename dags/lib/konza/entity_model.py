from __future__ import annotations

from pydantic_xml import BaseXmlModel, attr, element
from .common import XML_CONFIG
from .code import Code
from .effective_time import EffectiveTime
from pydantic import create_model
from typing import Optional, Type, ClassVar
from .assigned_entity import AssignedEntity


def entity_model(
    model_name: str,
    assigned_entity_field_name: str = "assignedEntity",
    assigned_entity_optional: bool = False,
    assigned_entity_class: Type = AssignedEntity,
    **kwargs
):
    
    kwargs[assigned_entity_field_name] = (
        assigned_entity_class, element(tag=assigned_entity_field_name)
    ) if not assigned_entity_optional else (
        Optional[assigned_entity_class], 
        element(tag=assigned_entity_field_name, default=None)
    )
    if not "functionCode" in kwargs:
        kwargs["functionCode"] = (Optional[Code], element(tag="functionCode", default=None))
    model = create_model(
        model_name,
        time=(Optional[EffectiveTime], element(tag="time", default=None)),
        __base__=BaseXmlModel,
        **kwargs
    )

    # Add xml_config for Pydantic v2
    model.xml_config = XML_CONFIG
    return model
