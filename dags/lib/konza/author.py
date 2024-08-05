from .entity_model import entity_model
from .effective_time import EffectiveTime
from .assigned_author import AssignedAuthor
from typing import Optional

Author = entity_model(
    "Author", 
    "assignedAuthor",
    assigned_entity_class=AssignedAuthor,
)
