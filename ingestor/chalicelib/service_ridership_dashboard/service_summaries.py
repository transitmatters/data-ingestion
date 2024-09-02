from typing import TypedDict, Optional
from datetime import date, timedelta

from .service_levels import ServiceLevelsEntry, ServiceLevelsByDate


class ServiceSummaryForDay(TypedDict):
    cancelled: bool
    tripsPerHour: Optional[list[int]]
    totalTrips: int


class ServiceSummary(TypedDict):
    weekday: ServiceSummaryForDay
    saturday: ServiceSummaryForDay
    sunday: ServiceSummaryForDay


def _is_matching_service_levels_entry(
    entry: ServiceLevelsEntry,
    valid_date_range_inclusive: tuple[date, date],
    matching_days_of_week: list[int],
    require_typical_service: bool,
) -> bool:
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
