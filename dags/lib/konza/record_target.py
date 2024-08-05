from pydantic_xml import BaseXmlModel
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .patient_role import PatientRole


class RecordTarget(BaseXmlModel, tag="recordTarget", **PYXML_KWARGS):
    patientRole: PatientRole
