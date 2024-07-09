from konza.entity_model import entity_model
from konza.effective_time import EffectiveTime
from konza.assigned_author import AssignedAuthor
from typing import Optional

Author = entity_model(
    "Author", 
    "assignedAuthor",
    assigned_entity_class=AssignedAuthor,
)
