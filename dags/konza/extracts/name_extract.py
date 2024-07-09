from pydantic_xml import BaseXmlModel
from typing import List, Optional
from konza.name import Name


def concat_tokens(tokens, empty_string=False):
    if len(tokens) == 0 and not empty_string:
        return None
    return " ".join([x.value for x in tokens])

class NameExtract(BaseXmlModel):
    name_prefix: Optional[str]
    given_name: str
    middle_names: str
    family_name: str
    name_suffix: Optional[str]


    @classmethod
    def from_name(cls, name: Name):
        return NameExtract(
            name_prefix=concat_tokens(name.prefix),
            given_name=concat_tokens(name.given),
            middle_names=concat_tokens(name.middle, empty_string=True),
            family_name=concat_tokens(name.family),
            name_suffix=concat_tokens(name.suffix),
        )
