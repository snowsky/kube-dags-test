from konza.entity_model import entity_model
from konza.assigned_custodian import AssignedCustodian

Custodian = entity_model(
    "Custodian",
    assigned_entity_field_name="assignedCustodian",
    assigned_entity_class=AssignedCustodian,
)
