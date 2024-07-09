from pydantic_xml import BaseXmlModel
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.patient_role import PatientRole


class RecordTarget(BaseXmlModel, tag="recordTarget", **PYXML_KWARGS):
    patientRole: PatientRole
