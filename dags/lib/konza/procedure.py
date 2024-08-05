from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional
from .code import Code
from typing import List
from .rim import ReferenceInformationModel
from .participant import Participant
from .specimen import Specimen
from .performer import Performer
from .extracts.procedure_extract import ProcedureExtract

class Procedure(ReferenceInformationModel, tag="procedure"):
    def as_procedure_extract(self) -> ProcedureExtract:
        
        code = None
        provider_id = None
        display_name = None
        original_text = None
        procedure_timestamp = None

        if self.code is not None:
            code = self.code.code
            display_name = self.code.displayName
            if self.code.originalText is not None:
                original_text = self.code.originalText.text
        if self.effectiveTime is not None:
            procedure_timestamp = self.effectiveTime.get_best_effort_interval_start()
        if len(self.performer) > 1:
            raise ValueError("Multiple performers found for procedure")
        if len(self.performer) == 1:
            performer = self.performer[0]
            if performer.assignedEntity is not None:
                npi_id = performer.assignedEntity.get_npi_id()
                if npi_id is not None:
                    provider_id = npi_id.extension
        return ProcedureExtract(
            code=code,
            provider_id=provider_id,
            display_name=display_name,
            original_text=original_text,
            procedure_timestamp=procedure_timestamp,
        )
