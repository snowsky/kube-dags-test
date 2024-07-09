from konza.assigned_entity import AssignedEntity
from konza.entity_model import entity_model
from pydantic_xml import element
from typing import Optional

Informant = entity_model(
    "Informant",
    assigned_entity_optional=True,
    relatedEntity=(Optional[AssignedEntity], element(tag="relatedEntity", default=None))
)
