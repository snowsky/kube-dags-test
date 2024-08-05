from pydantic_xml import BaseXmlModel, attr, element
from .patient import Patient
from .entity import Entity


class PatientRole(Entity):
    patient: Patient
