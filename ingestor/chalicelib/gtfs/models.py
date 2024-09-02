from typing import List, Dict
from dataclasses import dataclass
from datetime import date, datetime
from mbta_gtfs_sqlite.models import (
    CalendarService,
    CalendarAttribute,
    CalendarServiceException,
    Trip,
    Route,
)


@dataclass
class SessionModels:
    calendar_services: Dict[str, CalendarService]
    calendar_attributes: Dict[str, CalendarAttribute]
    calendar_service_exceptions: Dict[str, List[CalendarServiceException]]
    trips_by_route_id: Dict[str, Trip]
    routes: Dict[str, Route]


@dataclass
class RouteDateTotals:
    route_id: str
    line_id: str
    date: date
    count: int
    service_minutes: int
    by_hour: List[int]
    has_service_exceptions: bool

    @property
    def timestamp(self):
        dt = datetime.combine(self.date, datetime.min.time())
        return dt.timestamp()
