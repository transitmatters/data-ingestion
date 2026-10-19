"""Scheduled Commuter Rail service measured against TransitMatters' Regional Rail standards.

Regional Rail asks for frequent, all-day, bi-directional, clock-face service. These metrics are
computed from the schedule alone, so unlike trip events they are dense for every line and can be
backfilled across the whole GTFS archive.

Each direction is measured at its Boston terminal: outbound trips by when they leave Boston
(trip start), inbound trips by when they reach it (trip end). That uses only trip start/end times,
which the compact GTFS database keeps, and avoids having to pick a representative station per line.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from mbta_gtfs_sqlite.models import Trip

from .models import SessionModels
from .utils import get_service_ids_for_date_to_has_exceptions

REGIONAL_RAIL_TABLE_NAME = "ScheduledServiceRegionalRail"

OUTBOUND = "0"
INBOUND = "1"

HOUR = 60 * 60
# Regional Rail's baseline: a train at least every 30 minutes, all day, both directions
TARGET_HEADWAY = 30 * 60
# 5am to 1am, the span TransitMatters proposes for Regional Rail
SPAN_HOURS = range(5, 25)
WINDOWS = {"midday": (10 * HOUR, 15 * HOUR), "evening": (19 * HOUR, 24 * HOUR)}
AM_PEAK = (6 * HOUR, 10 * HOUR)
PM_PEAK = (16 * HOUR, 19 * HOUR)
CLOCK_FACE_TOLERANCE = 2 * 60


def is_commuter_rail_route(route_id: str) -> bool:
    return route_id.startswith("CR-")


def boston_times(trips: List[Trip], direction_id: str) -> List[int]:
    """Seconds after midnight that each trip in a direction leaves (outbound) or reaches (inbound) Boston."""
    if direction_id == OUTBOUND:
        return sorted(trip.start_time for trip in trips if trip.direction_id == OUTBOUND)
    return sorted(trip.end_time for trip in trips if trip.direction_id == INBOUND)


def max_gap(times: List[int], start: int, end: int) -> int:
    """Longest wait, in seconds, between trains that overlaps the window [start, end].

    The trains just before and just after the window count, so a 7am train followed by a 4pm
    train is a 9-hour gap at midday. With no train on one side, the window edge stands in.
    """
    before = [t for t in times if t <= start]
    inside = [t for t in times if start < t < end]
    after = [t for t in times if t >= end]
    points = [before[-1] if before else start, *inside, after[0] if after else end]
    return max(b - a for a, b in zip(points, points[1:]))


def hours_at_target(times: List[int]) -> int:
    """Hours between 5am and 1am in which no wait is longer than the Regional Rail target."""
    return sum(1 for hour in SPAN_HOURS if max_gap(times, hour * HOUR, (hour + 1) * HOUR) <= TARGET_HEADWAY)


def clock_face_share(times: List[int]) -> Optional[Decimal]:
    """Share of trains with another train exactly an hour before or after (within two minutes).

    A schedule that repeats every hour scores 1. Service every two hours scores 0: it repeats,
    but isn't the hourly pattern riders can memorize.
    """
    if len(times) < 2:
        return None
    repeating = sum(
        1
        for i, t in enumerate(times)
        if any(abs(abs(u - t) - HOUR) <= CLOCK_FACE_TOLERANCE for j, u in enumerate(times) if j != i)
    )
    return round(Decimal(repeating) / Decimal(len(times)), 3)


def count_in(times: List[int], window: tuple) -> int:
    start, end = window
    return sum(1 for t in times if start <= t < end)


def direction_metrics(times: List[int]) -> Dict:
    by_hour = [0] * 24
    for t in times:
        by_hour[(t // HOUR) % 24] += 1
    return {
        "trips": len(times),
        "firstTrip": times[0] if times else None,
        "lastTrip": times[-1] if times else None,
        **{f"maxGap{name.capitalize()}": max_gap(times, *window) // 60 for name, window in WINDOWS.items()},
        "hoursAtTarget": hours_at_target(times),
        "clockFaceShare": clock_face_share(times),
        "byHour": by_hour,
    }


def compute_regional_rail_metrics(trips: List[Trip]) -> Dict:
    """Regional Rail service metrics for one Commuter Rail route on one day."""
    outbound = boston_times(trips, OUTBOUND)
    inbound = boston_times(trips, INBOUND)
    return {
        "outbound": direction_metrics(outbound),
        "inbound": direction_metrics(inbound),
        # Into Boston in the morning and out in the evening is the commute the schedule is built
        # around; the reverse direction is what Regional Rail adds.
        "peakTrips": count_in(inbound, AM_PEAK) + count_in(outbound, PM_PEAK),
        "reversePeakTrips": count_in(outbound, AM_PEAK) + count_in(inbound, PM_PEAK),
    }


def create_regional_rail_items(today: date, models: SessionModels) -> List[Dict]:
    """DynamoDB items for every Commuter Rail route that runs in this feed, including no-service days."""
    service_ids = get_service_ids_for_date_to_has_exceptions(models, today)
    items = []
    for route_id, route in models.routes.items():
        route_trips = models.trips_by_route_id.get(route_id, [])
        if not is_commuter_rail_route(route_id) or not route_trips:
            continue
        trips = [trip for trip in route_trips if trip.service_id in service_ids]
        items.append(
            {
                "routeId": route_id,
                "lineId": route.line_id,
                "date": today.isoformat(),
                "timestamp": int(datetime.combine(today, datetime.min.time()).timestamp()),
                "hasServiceExceptions": any(service_ids[trip.service_id] for trip in trips),
                **compute_regional_rail_metrics(trips),
            }
        )
    return items
