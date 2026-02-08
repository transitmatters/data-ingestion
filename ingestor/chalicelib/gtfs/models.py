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
    """Container for GTFS data models loaded from a SQLite session.

    Attributes:
        calendar_services: Calendar services indexed by service ID.
        calendar_attributes: Calendar attributes indexed by service ID.
        calendar_service_exceptions: Calendar service exceptions bucketed by service ID.
        trips_by_route_id: Trips bucketed by route ID.
        routes: Routes indexed by route ID.
    """

    calendar_services: Dict[str, CalendarService]
    calendar_attributes: Dict[str, CalendarAttribute]
    calendar_service_exceptions: Dict[str, List[CalendarServiceException]]
    trips_by_route_id: Dict[str, Trip]
    routes: Dict[str, Route]


@dataclass
class RouteDateTotals:
    """Aggregated scheduled service totals for a single route on a single date.

    Attributes:
        route_id: The MBTA route identifier.
        line_id: The MBTA line identifier.
        date: The date these totals apply to.
        count: Total number of scheduled trips.
        service_minutes: Total scheduled service time in minutes.
        by_hour: List of 24 integers representing trip counts per hour of the day.
        has_service_exceptions: Whether any calendar service exceptions affect this date.
    """

    route_id: str
    line_id: str
    date: date
    count: int
    service_minutes: int
    by_hour: List[int]
    has_service_exceptions: bool

    @property
    def timestamp(self):
        """Compute a Unix timestamp from the date at midnight.

        Returns:
            The Unix timestamp as a float.
        """
        dt = datetime.combine(self.date, datetime.min.time())
        return dt.timestamp()
