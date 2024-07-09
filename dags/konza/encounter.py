from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.template_id import TemplateId
from typing import Optional
from konza.code import Code
from typing import List
from konza.rim import ReferenceInformationModel
from konza.sdtc_code import SDTCCode
from konza.extracts.encounter_extract import EncounterExtract

class Encounter(ReferenceInformationModel, tag="encounter"):
    
    dischargeDispositionCode: Optional[SDTCCode] = element(
        tag="dischargeDispositionCode", namespace="sdtc", default=None
    )
    def as_extract(self) -> EncounterExtract:
        
        service_datetime_start = None
        service_datetime_end = None
        
        if self.effectiveTime is not None:
            service_datetime_start = self.effectiveTime.get_best_effort_interval_start()
            service_datetime_end = self.effectiveTime.get_best_effort_interval_end()
        
        encounter_code = self.code.code
        encounter_code_display_name = self.code.displayName

        phin_vads_code = self.code.best_effort_phin_vads_code()
        encounter_phin_vads_code = phin_vads_code.code if phin_vads_code is not None else None
        encounter_phin_vads_code_display_name = phin_vads_code.displayName if phin_vads_code is not None else None
        discharged_time = service_datetime_end if encounter_phin_vads_code == "IMP" else None

        discharge_disposition_code = None
        discharge_disposition_display_name = None
        if self.dischargeDispositionCode is not None:
            discharge_disposition_code = self.dischargeDispositionCode.code
            discharge_disposition_display_name = self.dischargeDispositionCode.displayName

        dst_participant = self.get_dst_participant()
        discharged_to = None
        if (
            dst_participant is not None 
            and dst_participant.participantRole is not None
            and dst_participant.participantRole.playingEntity is not None
        ):
            discharged_to = dst_participant.participantRole.playingEntity.name

        return EncounterExtract(
            service_datetime_start=service_datetime_start,
            service_datetime_end=service_datetime_end,
            encounter_code=encounter_code,
            encounter_code_display_name=encounter_code_display_name,
            encounter_phin_vads_code=encounter_phin_vads_code,
            encounter_phin_vads_code_display_name=encounter_phin_vads_code_display_name,
            discharged_time=discharged_time,
            discharge_disposition_code=discharge_disposition_code,
            discharge_disposition_display_name=discharge_disposition_display_name,
            discharged_to=discharged_to,
        )

