from dataclasses import dataclass
from typing import Tuple, TypedDict, Literal, List
from datetime import date

StopPair = Tuple[str, str]
PeakType = Literal["all", "off_peak", "am_peak", "pm_peak"]
DirectionType = Literal["0", "1"]


@dataclass(frozen=True, eq=True)
class AlertsRequest(object):
    route: str
    date: date


class AlertsResponse(TypedDict):
    line: str
    date: date
    alerts: List[dict]


class Alert(TypedDict):
    valid_from: str
    valid_to: str
    text: str
