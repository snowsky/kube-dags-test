import xml.etree.ElementTree as ET

L1 = [
    "Encounters",
    "Problems",
    "Surgeries",
    "Allergies and Adverse Reactions",
    "Immunizations",
    "Plan of Care",
]
L2 = ["Medications"]
L3 = ["Vital Signs", "Diagnostic Results"]


def extract_field(XML_PATH: str, FIELD_NAME: str):
    tree = ET.parse(XML_PATH)
    root = tree.getroot()
    ns = "{urn:hl7-org:v3}"
    # Check field avaiability
    isField = (
        1
        if len(root.findall(f".//*/{ns}title[. = '{FIELD_NAME}']")) != 0
        else 0
    )
    if isField == 0:
        isField = (
            1
            if len(root.findall(f".//{ns}title[. = '{FIELD_NAME.upper()}']"))
            != 0
            else 0
        )

    if isField == 1:
        if FIELD_NAME in L1:
            sign = "SNOMED-CT"
        elif FIELD_NAME in L2:
            sign = "RxNorm"
        elif FIELD_NAME in L3:
            sign = "LOINC"
        else:
            sign = " "
        extracted = ""

        for t in root.findall(f".//*[{ns}title='{FIELD_NAME}']")[0].findall(
            f".//*/{ns}td"
        ):
            text = t.text if t.text is not None else "NA"
            extracted += ";" + text
        field_code = [e for e in extracted.split(";")[1:] if e.find(" ") != -1]
        code = [c for c in field_code if c.find(sign) != -1]
        code = "; ".join(code) if len(code) != 0 else "NA"
        field = [c for c in field_code if c.find(sign) == -1]
        field = "; ".join(field) if len(field) != 0 else "NA"
        return {FIELD_NAME: field, "code": code}
    else:
        return "NA"


fieldsList = [
    "Encounters",
    "Insurance",
    "Providers",
    "Problems",
    "Procedures",
    "Surgeries",
    "Medications",
    "Allergies",
    "Allergies and Adverse Reactions",
    "Immunizations",
    "Vital Signs",
    "Lab",
    "Diagnostic Results",
    "Social History",
    "Plan of Care",
]


def parser_other(XML_PATH):
    others = {}
    for field in fieldsList:
        others[field] = extract_field(XML_PATH, field)
    return others
