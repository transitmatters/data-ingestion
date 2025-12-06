from dataclasses import dataclass
from datetime import date
from typing import List, Literal, Tuple, TypedDict

StopPair = Tuple[str, str]
PeakType = Literal["all", "off_peak", "am_peak", "pm_peak"]
DirectionType = Literal["0", "1"]


@dataclass(frozen=True, eq=True)
class AggTravelTimesRequest(object):
    route_id: str
    includes_terminals: bool
    direction: Literal["0", "1"]
    stop_pair: StopPair
    start_date: date
    end_date: date


class AggTravelTimesByDateEntry(TypedDict):
    count: int
    max: int
    mean: float
    min: int
    std: float
    peak: PeakType
    service_date: str


AggTravelTimesResponse = List[AggTravelTimesByDateEntry]
