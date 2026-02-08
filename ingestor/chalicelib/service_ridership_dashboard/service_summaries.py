from datetime import date, timedelta
from typing import Optional

from .service_levels import ServiceLevelsByDate, ServiceLevelsEntry
from .types import ServiceSummary, ServiceSummaryForDay


def _is_matching_service_levels_entry(
    entry: ServiceLevelsEntry,
    valid_date_range_inclusive: tuple[date, date],
    matching_days_of_week: list[int],
    require_typical_service: bool,
) -> bool:
    """Check whether a service levels entry matches the given date range, day-of-week, and service criteria.

    Args:
        entry: The service levels entry to check.
        valid_date_range_inclusive: A tuple of (start_date, end_date) defining the valid range.
        matching_days_of_week: A list of weekday integers (0=Monday, 6=Sunday) to match.
        require_typical_service: If True, exclude entries with service exceptions.

    Returns:
        True if the entry matches all criteria.
    """
    valid_start_date, valid_end_date = valid_date_range_inclusive
    return (
        valid_start_date <= entry.date <= valid_end_date
        and entry.date.weekday() in matching_days_of_week
        and (not require_typical_service or not entry.has_service_exceptions)
    )


def _get_matching_service_levels_entry(
    service_levels: ServiceLevelsByDate,
    start_lookback_date: date,
    max_lookback_days: int,
    matching_days_of_week: list[int],
    require_typical_service: bool,
) -> Optional[ServiceLevelsEntry]:
    """Find the most recent service levels entry matching the given criteria within a lookback window.

    Args:
        service_levels: A dictionary mapping dates to service level entries.
        start_lookback_date: The date to start looking back from.
        max_lookback_days: The maximum number of days to look back.
        matching_days_of_week: A list of weekday integers (0=Monday, 6=Sunday) to match.
        require_typical_service: If True, only match entries without service exceptions.

    Returns:
        The most recent matching ServiceLevelsEntry, or None if no match is found.
    """
    end_lookback_date = start_lookback_date - timedelta(days=max_lookback_days)
    for lookback_date in sorted(service_levels.keys(), reverse=True):
        if _is_matching_service_levels_entry(
            service_levels[lookback_date],
            (end_lookback_date, start_lookback_date),
            matching_days_of_week,
            require_typical_service,
        ):
            return service_levels[lookback_date]
    return None


def _is_service_cancelled_on_date(
    service_levels: ServiceLevelsByDate,
    start_lookback_date: date,
    matching_days_of_week: list[int],
) -> bool:
    """Determine whether service is cancelled for the given days of week near the lookback date.

    Args:
        service_levels: A dictionary mapping dates to service level entries.
        start_lookback_date: The date to check around.
        matching_days_of_week: A list of weekday integers (0=Monday, 6=Sunday) to check.

    Returns:
        True if no matching service entry is found within the past 7 days.
    """
    return (
        _get_matching_service_levels_entry(
            service_levels=service_levels,
            start_lookback_date=start_lookback_date,
            matching_days_of_week=matching_days_of_week,
            require_typical_service=False,
            max_lookback_days=7,
        )
        is None
    )


def _get_service_levels_summary_dict(
    start_lookback_date: date,
    service_levels: ServiceLevelsByDate,
    matching_days_of_week: list[int],
) -> ServiceSummaryForDay:
    """Build a service summary for a specific day type (weekday, Saturday, or Sunday).

    Args:
        start_lookback_date: The date to look back from when finding service data.
        service_levels: A dictionary mapping dates to service level entries.
        matching_days_of_week: A list of weekday integers (0=Monday, 6=Sunday) to match.

    Returns:
        A ServiceSummaryForDay dict with cancellation status, trips per hour, and total trips.
    """
    if _is_service_cancelled_on_date(
        service_levels=service_levels,
        start_lookback_date=start_lookback_date,
        matching_days_of_week=matching_days_of_week,
    ):
        return {
            "cancelled": True,
            "tripsPerHour": None,
            "totalTrips": 0,
        }
    service_levels_entry = _get_matching_service_levels_entry(
        start_lookback_date=start_lookback_date,
        service_levels=service_levels,
        matching_days_of_week=matching_days_of_week,
        require_typical_service=True,
        max_lookback_days=(1000 * 365),
    )
    assert service_levels_entry is not None
    return {
        "cancelled": False,
        "tripsPerHour": service_levels_entry.service_levels,
        "totalTrips": round(sum(service_levels_entry.service_levels)),
    }


def summarize_weekly_service_around_date(date: date, service_levels: ServiceLevelsByDate) -> ServiceSummary:
    """Create a weekly service summary with breakdowns for weekday, Saturday, and Sunday.

    Args:
        date: The reference date to summarize service around.
        service_levels: A dictionary mapping dates to service level entries.

    Returns:
        A ServiceSummary dict with weekday, Saturday, and Sunday service summaries.
    """
    weekday_service = _get_service_levels_summary_dict(
        service_levels=service_levels,
        start_lookback_date=date,
        matching_days_of_week=list(range(0, 5)),
    )
    saturday_service = _get_service_levels_summary_dict(
        service_levels=service_levels,
        start_lookback_date=date,
        matching_days_of_week=[5],
    )
    sunday_service = _get_service_levels_summary_dict(
        service_levels=service_levels,
        start_lookback_date=date,
        matching_days_of_week=[6],
    )
    return {
        "weekday": weekday_service,
        "saturday": saturday_service,
        "sunday": sunday_service,
    }
