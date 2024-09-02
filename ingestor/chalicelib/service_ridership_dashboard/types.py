from typing import TypedDict, Literal, Optional

LineKind = Literal[
    "bus",
    "regional-rail",
    "silver",
    "red",
    "orange",
    "blue",
    "green",
    "boat",
]


class ServiceSummaryForDay(TypedDict):
    cancelled: bool
    tripsPerHour: Optional[list[int]]
    totalTrips: int


class ServiceSummary(TypedDict):
    weekday: ServiceSummaryForDay
    saturday: ServiceSummaryForDay
    sunday: ServiceSummaryForDay


class ServiceRegimes(TypedDict):
    current: ServiceSummary
    oneYearAgo: ServiceSummary
    baseline: ServiceSummary  # This is the pre-covid service level from CRD


class LineData(TypedDict):
    id: str
    shortName: str
    longName: str
    routeIds: list[str]
    startDate: str
    lineKind: LineKind
    ridershipHistory: list[float]
    serviceHistory: list[float]
    serviceRegimes: ServiceRegimes


class SummaryData(TypedDict):
    totalRidershipHistory: list[float]
    totalServiceHistory: list[float]
    totalRidershipPercentage: float
    totalServicePercentage: float
    totalPassengers: float
    totalTrips: float
    totalRoutesCancelled: int
    totalReducedService: int
    totalIncreasedService: int
    startDate: str
    endDate: str


class DashJSON(TypedDict):
    lineData: dict[str, LineData]
    summaryData: SummaryData
