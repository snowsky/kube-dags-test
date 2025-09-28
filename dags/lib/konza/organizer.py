from __future__ import annotations

from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import XML_CONFIG
from .template_id import TemplateId
from typing import Optional, ClassVar
from .code import Code
from typing import List, ClassVar
from .rim import ReferenceInformationModel
from .extracts.care_team_extract import CareTeamExtract
from .organizer_component import OrganizerComponent
from .act import Act

class Organizer(ReferenceInformationModel, tag="organizer"):
    
    component: List[OrganizerComponent] = element(tag="component", default=[])

    def get_pcpr_act(self) -> Optional[Act]:
        for component in self.component:
            if component.act is not None and component.act.is_primary_care_provision():
                return component.act

    def as_care_team_extract(self) -> CareTeamExtract:
        
        attributed_npi = None 
        fname = None
        lname = None
        prov_type = None
        suffix = None

        pcpr_act = self.get_pcpr_act()
        if pcpr_act is not None:
            if len(pcpr_act.performer) > 1:
                raise ValueError("Multiple primary care performers found.")
            if len(pcpr_act.performer) == 1:
                performer = pcpr_act.performer[0]
                prov_type = performer.functionCode.code
                if performer.assignedEntity is not None:
                    npi_id = performer.assignedEntity.get_npi_id()
                    if npi_id is not None:
                        attributed_npi = npi_id.extension
                    if performer.assignedEntity.assignedPerson is not None:
                        person = performer.assignedEntity.assignedPerson
                        if person.name is not None:
                           fname = person.name.get_first_name_no_qualifiers()
                           mname = person.name.get_middle_name_no_qualifiers()
                           lname = person.name.get_last_name_no_qualifiers()
                           suffix = person.name.get_suffix_no_qualifiers()

        return CareTeamExtract(
            attributed_npi=attributed_npi,
            fname=fname,
            lname=lname,
            mname=mname,
            prov_type=prov_type,
            suffix=suffix,
        )

            
