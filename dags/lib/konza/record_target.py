from __future__ import annotations

from pydantic_xml import BaseXmlModel
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .patient_role import PatientRole


class RecordTarget(BaseXmlModel):
    xml_config: ClassVar = XML_CONFIG
    patientRole: PatientRole
