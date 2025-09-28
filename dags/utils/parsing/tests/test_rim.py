import sys
import os
# Add the dags directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from lib.konza.parser import read_clinical_document_from_xml_path
from lib.konza.extracts.extract import KonzaExtract
from lib.konza.code import Code
from lib.konza.template_id import TemplateId
from lib.konza.text import Text
from lib.konza.effective_time import EffectiveTime
from lib.konza.value import Value
from lib.konza.author import Author
from lib.konza.assigned_entity import AssignedEntity
from lib.konza.assigned_author import AssignedAuthor
from lib.konza.addr import Addr
from lib.konza.telecom import Telecom
from lib.konza.person import Person
from lib.konza.name import Name
from lib.konza.token import Token
from lib.konza.authoring_device import AuthoringDevice
from lib.konza.organization import Organization

def test_basic_extract():
    XML_PATH = "ccda/CCD.xml"
    d = read_clinical_document_from_xml_path(XML_PATH)
    assert d.realmCode == Code(code="US")    
    assert d.typeId == TemplateId(
        extension="POCD_HD000040",
        root="2.16.840.1.113883.1.3"
    )
    assert d.templateId == TemplateId(
        root="2.16.840.1.113883.10.20.22.1.2",
        extension="2014-06-09"
    )
    assert d.id == TemplateId(
        extension="EHRVersion2.0",
        root="be84a8e4-a22e-4210-a4a6-b3c48273e84c"
    )
    assert d.code == Code(
        code="34133-9",
        displayName="Summary of episode note",
        codeSystem="2.16.840.1.113883.6.1",
        codeSystemName="LOINC",
    )
    assert d.title == "Summary of Patient Chart"
    assert d.effectiveTime == EffectiveTime(
        value="20141015103026-0500",
    )
    assert d.confidentialityCode == Code(
        code="N",
        displayName="normal",
        codeSystem="2.16.840.1.113883.5.25",
        codeSystemName="Confidentiality",
    )
    assert d.languageCode == Code(code="en-US")
    assert d.setId == TemplateId(extension="sTT988",root="2.16.840.1.113883.19.5.99999.19")
    assert d.versionNumber == Value(value="1")
    assert len(d.author) == 2

    assert d.author[0] == Author(
		    time=EffectiveTime(value="20141015103026-0500"),
        assignedAuthor=AssignedAuthor(
			      id=[TemplateId(extension="5555555555", root="2.16.840.1.113883.4.6")],
            code=Code(
                code="207QA0505X",
                displayName="Allopathic & Osteopathic Physicians; Family Medicine, Adult Medicine",
                codeSystem="2.16.840.1.113883.6.101",
                codeSystemName="Healthcare Provider Taxonomy (HIPAA)",
            ),
            addr=Addr(
                streetAddressLine=["1004 Healthcare Drive "],
                city="Portland",
                state="OR",
                postalCode="99123",
                country="US",
            ),
            telecom=[Telecom(use="WP", value="tel:+1(555)555-1004")],
            assignedPerson=Person(
                name=Name(
                    given=[
                        Token(value="Patricia"),
                        Token(qualifier="CL", value="Patty"),
                    ],
                    family=[Token(value="Primary")],
                    suffix=[Token(qualifier="AC", value="M.D.")]
                )
            )
        )
    )

    assert d.author[1] == Author(
		    time=EffectiveTime(value="20141015103026-0500"),
        assignedAuthor=AssignedAuthor(
			      id=[TemplateId(nullFlavor="NI")],
            assigned_authoring_device=AuthoringDevice(
                manufacturerModelName="Generic EHR Clinical System 2.0.0.0.0.0",
                softwareName="Generic EHR C-CDA Factory 2.0.0.0.0.0 - C-CDA Transform 2.0.0.0.0",
            ),
            addr=Addr(
                streetAddressLine=["1004 Healthcare Drive "],
                city="Portland",
                state="OR",
                postalCode="99123",
                country="US",
            ),
            telecom=[Telecom(use="WP", value="tel:+1(555)555-1004")],
            representedOrganization=Organization(
				        id=[TemplateId(extension="3", root="1.3.6.1.4.1.22812.3.99930.3")],
                name="The Doctors Together Physician Group",
                addr=Addr(
                    streetAddressLine=["1004 Healthcare Drive "],
                    city="Portland",
                    state="OR",
                    postalCode="99123",
                    country="US",
                ),
                telecom=[Telecom(value="tel:+1(555)555-1004")],
            ), 
        )
    )
    components = d.component[0].structuredBody.component
    assert len(components) == 7
    
    assert len(components[0].section) == 1
    section_0 = components[0].section[0]

    assert len(section_0.entry) == 1
    assert len(section_0.entry[0].events) == 1
    event = section_0.entry[0].events[0]
    assert len(components) == 7
    
    assert len(components[0].section) == 1
    section_0 = components[0].section[0]

    assert len(section_0.entry) == 1
    assert len(section_0.entry[0].events) == 1
    event = section_0.entry[0].events[0]

    components = d.component[0].structuredBody.component

