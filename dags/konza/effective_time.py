from pydantic_xml import BaseXmlModel, element, attr
from lxml.etree import _Element as Element
from konza.common import PYXML_KWARGS
from konza.value import Value
from typing import Optional


class EffectiveTime(BaseXmlModel, tag="effectiveTime", **PYXML_KWARGS):
    nullFlavor: Optional[str] = attr(tag="nullFlavor", default=None) 
    value: Optional[str] = attr(tag="value", default=None)
    low: Optional[Value] = element(name="low", default=None)
    center: Optional[Value] = element(name="center", default=None)
    high: Optional[Value] = element(name="high", default=None)

    def get_best_effort_interval_start(self) -> Optional[str]:
        if self.low is not None and self.low.value is not None:
            return self.low.value
        elif self.center is not None and self.center.value is not None:
            return self.center
        return self.value
    
    def get_best_effort_interval_end(self) -> Optional[str]:
        if self.high is not None and self.high.value is not None:
            return self.high.value
        return None
