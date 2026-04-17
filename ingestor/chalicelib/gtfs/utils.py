from datetime import date, timedelta
from typing import List, Dict, Union, Any, Callable, Generator, TYPE_CHECKING
from mbta_gtfs_sqlite.models import (
    Trip,
    CalendarServiceExceptionType,
    ServiceDayAvailability,
)

if TYPE_CHECKING:
    from models import SessionModels


def bucket_trips_by_hour(trips: List[Trip]) -> List[int]:
    """Count trips by their starting hour of the day.

    Args:
        trips: List of Trip objects with start_time in seconds since midnight.

    Returns:
        A list of 24 integers, where each element is the number of trips
        starting in that hour.
    """
    by_time_of_day = [0] * 24
    for trip in trips:
        hour = (trip.start_time // 3600) % 24
        by_time_of_day[hour] += 1
    return by_time_of_day


def get_total_service_minutes(trips: List[Trip]) -> int:
    """Calculate the total service time in minutes across all trips.

    Args:
        trips: List of Trip objects with start_time and end_time in seconds.

    Returns:
        The total service duration in minutes as an integer.
    """
    return sum(trip.end_time - trip.start_time for trip in trips) // 60


def is_valid_route_id(route_id: str) -> bool:
    """Check whether a route ID represents a valid (non-shuttle) route.

    Args:
        route_id: The MBTA route identifier string.

    Returns:
        True if the route is valid, False if it is a shuttle route or
        the Green Line shuttle (route 602).
    """
    return not route_id.startswith("Shuttle") and route_id != "602"  # 602 is a Green Line shuttle


def bucket_by(
    items: List[any],
    key_getter: Union[str, Callable[[Any], str]],
) -> Dict[str, List[any]]:
    """Group items into lists by a key derived from each item.

    Args:
        items: List of items to group.
        key_getter: A callable that extracts the grouping key from an item,
            or a string key for dictionary-style access.

    Returns:
        A dictionary mapping each key to a list of items with that key.
    """
    res = {}
    if isinstance(key_getter, str):
        key_getter_as_str = key_getter
        key_getter = lambda dict: dict[key_getter_as_str]
    for item in items:
        key = key_getter(item)
        res.setdefault(key, [])
        res[key].append(item)
    return res


def index_by(items: List[any], key_getter: Union[str, Callable[[Any], str]]) -> Dict[str, Any]:
    """Index items into a dictionary by a key derived from each item.

    If multiple items share the same key, the last item overwrites previous ones.

    Args:
        items: List of items to index.
        key_getter: A callable that extracts the index key from an item,
            or a string key for dictionary-style access.

    Returns:
        A dictionary mapping each key to a single item.
    """
    res = {}
    if isinstance(key_getter, str):
        key_getter_as_str = key_getter
        key_getter = lambda dict: dict[key_getter_as_str]
    for item in items:
        key = key_getter(item)
        res[key] = item
    return res


def date_range(start_date: date, end_date: date) -> Generator[date, None, None]:
    """Yield each date from start_date to end_date inclusive.

    Args:
        start_date: The first date in the range.
        end_date: The last date in the range (inclusive).

    Yields:
        Each date from start_date through end_date.
    """
    assert start_date <= end_date
    now = start_date
    while now <= end_date:
        yield now
        now = now + timedelta(days=1)


def get_service_ids_for_date_to_has_exceptions(models: "SessionModels", today: date) -> dict[str, bool]:
    """Reports a dict of service IDs that are active on the given date mapped to a boolean indicating if
    there are any exceptions for that service on that date.

    Args:
        models: The SessionModels containing calendar services and exceptions.
        today: The date to evaluate service availability for.

    Returns:
        A dictionary mapping active service IDs to a boolean indicating whether
        any calendar service exceptions apply on that date.
    """
    services_for_today: dict[str, bool] = {}
    for service_id in models.calendar_services.keys():
        service = models.calendar_services.get(service_id)
        if not service:
            continue
        service_exceptions = models.calendar_service_exceptions.get(service_id, [])
        in_range = service.start_date <= today <= service.end_date
        on_sevice_day = [
            service.monday,
            service.tuesday,
            service.wednesday,
            service.thursday,
            service.friday,
            service.saturday,
            service.sunday,
        ][today.weekday()] == ServiceDayAvailability.AVAILABLE
        service_exceptions_today = [ex for ex in service_exceptions if ex.date == today]
        is_removed_by_exception = any(
            (ex.exception_type == CalendarServiceExceptionType.REMOVED for ex in service_exceptions_today)
        )
        is_added_by_exception = any(
            (ex.exception_type == CalendarServiceExceptionType.ADDED for ex in service_exceptions_today)
        )
        if is_added_by_exception or (in_range and on_sevice_day and not is_removed_by_exception):
            services_for_today[service_id] = len(service_exceptions_today) > 0
    return services_for_today
