from .rim import ReferenceInformationModel
from typing import Self, Optional
from .extracts.insurance_extract import InsuranceExtract
from .extracts.problem_extract import ProblemExtract
from .extracts.allergy_extract import AllergyExtract
from .observation import Observation
from .template_id import TemplateId

ACT_PCPR_CODE = "PCPR"

class Act(ReferenceInformationModel, tag="act"):
    
    def is_policy_activity(self) -> bool:
        return self.check_template_ids(TemplateId.is_policy_activity)

    def is_primary_care_provision(self) -> bool:
        return self.classCode == ACT_PCPR_CODE

    def is_date_of_diagnosis_act(self) -> bool:
        self.check_template_ids(TemplateId.is_date_of_diagnosis)

    def get_policy_activity_if_any(self) -> Optional[Self]:
        if self.is_policy_activity():
            return self
        
        for entryRelationship in self.entryRelationship:
            for event in entryRelationship.events:
                if event.is_policy_activity():
                    return event
        return None
    
    def as_insurance_extract(self) -> InsuranceExtract:

        policy_activity = self.get_policy_activity_if_any()
        grpid = None
        if policy_activity is not None and policy_activity.id is not None:
            grpid = policy_activity.id.root

        cov = self.get_cov_participant()
        eff_date = None
        exp_date = None
        if cov is not None and cov.time is not None:
            eff_date = cov.time.low
            exp_date = cov.time.high
            
        plan_type = None
        if policy_activity is not None and policy_activity.code is not None:
            plan_type = policy_activity.code.code

        return InsuranceExtract(
            grpid=grpid,
            eff_date=eff_date,
            exp_date=exp_date,
            plan_type=plan_type,
        )

    def as_problem_extract(self) -> ProblemExtract:
        
        code = None
        observation_extracts = []
        for entry in self.entryRelationship:
            for event in entry.events:
                if isinstance(event, Observation):
                    observation_extract = event.as_problem_observation_extract()
                    observation_extracts.append(observation_extract)
        return ProblemExtract(
            observations=observation_extracts,
        )
    
    def as_allergy_extract(self) -> AllergyExtract:
        
        code = None
        observation_extracts = []
        for entry in self.entryRelationship:
            for event in entry.events:
                if isinstance(event, Observation):
                    observation_extract = event.as_allergy_observation_extract()
                    observation_extracts.append(observation_extract)
        return AllergyExtract(
            observations=observation_extracts,
        )
