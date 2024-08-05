from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from .common import PYXML_KWARGS
from .template_id import TemplateId
from typing import Optional, Self
from .code import Code
from typing import List
from .rim import ReferenceInformationModel
from .quantity import Quantity
from .consumable import Consumable
from .extracts.substance_administration_extract import SubstanceAdministrationExtract
from .extracts.immunization_extract import ImmunizationExtract

class SubstanceAdministration(
    ReferenceInformationModel, tag="substanceAdministration"
):
    routeCode: Optional[Code] = element(tag="routeCode", default=None)
    doseQuantity: Optional[Quantity] = element(tag="doseQuantity", default=None)
    consumable: Optional[Consumable] = element(tag="consumable", default=None)
    repeatNumber: Optional[str] = element(tag="repeatNumber", default=None)

    def is_medication_instructions(self) -> bool:
        return (
            self.code is not None and
            self.code.is_medication_instructions()
        )

    def get_medication_instructions_if_any(self) -> Optional[Self]:
        for entry in self.entryRelationship:
            for event in entry.events:
                if isinstance(event, SubstanceAdministration) and event.is_medication_instructions():
                    return event
        return None

    def as_substance_administration_extract(self) -> SubstanceAdministrationExtract:
        
        normalized_code = None
        normalized_codesystem = None
        name = None
        route = None
        sig = None
        # TODO: these are not specified in C-143
        strength = None
        packaging = None
        dosage = None

        if self.consumable is not None:
            product = self.consumable.manufacturedProduct
            if product.manufacturedMaterial is not None:
                code = product.manufacturedMaterial.code
                normalized_code = code.code
                normalized_codesystem = code.codeSystem
                name = code.displayName
        if self.routeCode is not None:
            route = self.routeCode.displayName
       
        medication_instructions = self.get_medication_instructions_if_any()
        if medication_instructions is not None:
            sig = medication_instructions.text

        return SubstanceAdministrationExtract(
            normalized_code=normalized_code,
            normalized_codesystem=normalized_codesystem,
            name=name,
            route=route,
            sig=sig,
            strength=strength,
            packaging=packaging,
            dosage=dosage,
        )
    
    def as_immunization_extract(self) -> ImmunizationExtract:

        normalized_code = None
        long_name = None
        lot_number = None
        performing_provider_id = None

        if self.consumable is not None:
            product = self.consumable.manufacturedProduct
            if product.manufacturedMaterial is not None:
                code = product.manufacturedMaterial.code
                normalized_code = code.code
                long_name = code.displayName
                lot_number = (
                    product.manufacturedMaterial.lotNumberText.value 
                    if product.manufacturedMaterial.lotNumberText is not None else None
                )
        if len(self.performer) > 1:
            raise ValueError("Multiple performers unexpectedly found for immunization.")
        if len(self.performer) == 1:
            if self.performer[0].assignedEntity is not None:
                performing_provider_id = self.performer[0].assignedEntity.id[0].root


        return ImmunizationExtract(
            performing_provider_id=performing_provider_id,
            series_number=self.repeatNumber,
            administered_date=self.effectiveTime.value if self.effectiveTime is not None else None,
            lot_number=lot_number,
            long_name=long_name,
            code=normalized_code,
            source=None,
            vault_based_id=None,
            administered_code_id=None,
            event_timestamp=None,
            immunization_id=None,
            vaccine_id=None,
            vaccine_manufacturer_id=None,
            manufacturer_text=None,
            vaccination_status_id=None,
            dosage=None,
            measure_unit_id=None,
            display=None,
            description=None,
            short_name=None,
        )
