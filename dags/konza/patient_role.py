from pydantic_xml import BaseXmlModel, attr, element
from konza.patient import Patient
from konza.entity import Entity


class PatientRole(Entity):
    patient: Patient
