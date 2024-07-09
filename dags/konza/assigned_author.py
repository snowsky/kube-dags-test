from pydantic_xml import element
from konza.assigned_entity_model import assigned_entity_model
from konza.authoring_device import AuthoringDevice
from typing import Optional

AssignedAuthor = assigned_entity_model(
    "AssignedAuthor",
    assigned_authoring_device=(
        Optional[AuthoringDevice], element(tag="assignedAuthoringDevice", default=None),
    ),
)
