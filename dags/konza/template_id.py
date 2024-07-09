from pydantic_xml import BaseXmlModel, attr
from konza.common import PYXML_KWARGS
from typing import Optional

# POLICY_ACTIVITY_TEMPLATE_ROOT = "2.16.840.1.113883.10.20.22.4.61"
POLICY_ACTIVITY_TEMPLATE_ROOT = "2.16.840.1.113883.10.20.22.4.60"

US_NPI_ROOT = "2.16.840.1.113883.4.6"
LOINC_PROBLEM_OBSERVATION_ROOT = "2.16.840.1.113883.10.20.22.4.4"
DATE_OF_DIAGNOSIS_ROOT = "2.16.840.1.113883.10.20.22.4.502"

class TemplateId(BaseXmlModel, tag="templateId", **PYXML_KWARGS):
    root: Optional[str] = attr(default=None)
    extension: Optional[str] = attr(default=None)

    def _check_root(self, expected_val: str) -> bool:
        if self.root is None:
            return False
        return self.root == expected_val
    
    def is_policy_activity(self):
        return self._check_root(POLICY_ACTIVITY_TEMPLATE_ROOT)
    
    def is_npi(self):
        return self._check_root(US_NPI_ROOT)

    def is_problem_observation(self):
        return self._check_root(LOINC_PROBLEM_OBSERVATION_ROOT)

    def is_date_of_diagnosis(self):
        return self._check_root(DATE_OF_DIAGNOSIS_ROOT)