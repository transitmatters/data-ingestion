from dataclasses import dataclass
from datetime import date
from typing import List, Literal, Tuple, TypedDict

StopPair = Tuple[str, str]
PeakType = Literal["all", "off_peak", "am_peak", "pm_peak"]
DirectionType = Literal["0", "1"]


@dataclass(frozen=True, eq=True)
class AggTravelTimesRequest(object):
    """Immutable request object for fetching aggregated travel times.

    Attributes:
        route_id: The route identifier string (e.g. "line-red").
        includes_terminals: Whether the stop pair includes terminal stations.
        direction: The direction of travel, "0" or "1".
        stop_pair: A tuple of (from_stop, to_stop) station identifiers.
        start_date: The start date of the query range.
        end_date: The end date of the query range.
    """

    route_id: str
    includes_terminals: bool
    direction: Literal["0", "1"]
    stop_pair: StopPair
    start_date: date
    end_date: date


class AggTravelTimesByDateEntry(TypedDict):
    """Typed dictionary representing a single date entry of aggregated travel time statistics.

    Attributes:
        count: The number of trips observed.
        max: The maximum travel time in seconds.
        mean: The mean travel time in seconds.
        min: The minimum travel time in seconds.
        std: The standard deviation of travel times.
        peak: The peak period classification.
        service_date: The service date as a string.
    """

    count: int
    max: int
    mean: float
    min: int
    std: float
    peak: PeakType
    service_date: str


AggTravelTimesResponse = List[AggTravelTimesByDateEntry]
