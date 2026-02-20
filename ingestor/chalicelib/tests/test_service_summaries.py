from datetime import date, timedelta

import pytest

from ..service_ridership_dashboard.service_levels import ServiceLevelsEntry
from ..service_ridership_dashboard.service_summaries import (
    _get_matching_service_levels_entry,
    _get_service_levels_summary_dict,
    _is_matching_service_levels_entry,
    _is_service_cancelled_on_date,
    summarize_weekly_service_around_date,
)


def make_entry(
    entry_date: date,
    has_service_exceptions: bool = False,
    service_levels: list[int] = None,
) -> ServiceLevelsEntry:
    return ServiceLevelsEntry(
        line_id="line-Red",
        line_short_name="Red",
        line_long_name="Red Line",
        route_ids=["Red"],
        service_levels=service_levels or [2] * 24,
        has_service_exceptions=has_service_exceptions,
        date=entry_date,
    )


def make_service_levels(*entries: ServiceLevelsEntry) -> dict:
    return {e.date: e for e in entries}


# --- _is_matching_service_levels_entry ---


def test_is_matching_in_range_correct_day():
    monday = date(2024, 1, 1)  # Monday (weekday 0)
    entry = make_entry(monday)
    assert _is_matching_service_levels_entry(entry, (monday, monday), [0], False) is True


def test_is_matching_out_of_range():
    monday = date(2024, 1, 1)
    entry = make_entry(monday)
    later_range = (date(2024, 2, 1), date(2024, 2, 28))
    assert _is_matching_service_levels_entry(entry, later_range, [0], False) is False


def test_is_matching_wrong_day_of_week():
    monday = date(2024, 1, 1)  # Monday = 0
    entry = make_entry(monday)
    assert _is_matching_service_levels_entry(entry, (monday, monday), [5, 6], False) is False


def test_is_matching_exceptions_with_require_typical_true():
    monday = date(2024, 1, 1)
    entry = make_entry(monday, has_service_exceptions=True)
    assert _is_matching_service_levels_entry(entry, (monday, monday), [0], True) is False


def test_is_matching_exceptions_with_require_typical_false():
    monday = date(2024, 1, 1)
    entry = make_entry(monday, has_service_exceptions=True)
    assert _is_matching_service_levels_entry(entry, (monday, monday), [0], False) is True


# --- _get_matching_service_levels_entry ---


def test_get_matching_finds_entry_within_lookback():
    monday = date(2024, 1, 8)  # Monday
    entry = make_entry(monday)
    service_levels = make_service_levels(entry)
    result = _get_matching_service_levels_entry(
        service_levels=service_levels,
        start_lookback_date=monday,
        max_lookback_days=7,
        matching_days_of_week=[0],
        require_typical_service=False,
    )
    assert result is entry


def test_get_matching_returns_none_outside_lookback():
    old_monday = date(2024, 1, 1)  # 3 weeks ago
    entry = make_entry(old_monday)
    service_levels = make_service_levels(entry)
    result = _get_matching_service_levels_entry(
        service_levels=service_levels,
        start_lookback_date=date(2024, 1, 22),
        max_lookback_days=7,
        matching_days_of_week=[0],
        require_typical_service=False,
    )
    assert result is None


def test_get_matching_returns_most_recent():
    d1 = date(2024, 1, 1)  # Monday
    d2 = date(2024, 1, 8)  # Monday
    e1 = make_entry(d1)
    e2 = make_entry(d2)
    service_levels = make_service_levels(e1, e2)
    result = _get_matching_service_levels_entry(
        service_levels=service_levels,
        start_lookback_date=d2,
        max_lookback_days=14,
        matching_days_of_week=[0],
        require_typical_service=False,
    )
    assert result is e2


def test_get_matching_skips_exceptions_when_require_typical():
    monday = date(2024, 1, 8)
    entry = make_entry(monday, has_service_exceptions=True)
    service_levels = make_service_levels(entry)
    result = _get_matching_service_levels_entry(
        service_levels=service_levels,
        start_lookback_date=monday,
        max_lookback_days=7,
        matching_days_of_week=[0],
        require_typical_service=True,
    )
    assert result is None


# --- _is_service_cancelled_on_date ---


def test_is_service_cancelled_no_entries_in_7_days():
    old_monday = date(2024, 1, 1)
    entry = make_entry(old_monday)
    service_levels = make_service_levels(entry)
    # Look from 3 weeks later — no entries within 7 days
    assert _is_service_cancelled_on_date(service_levels, date(2024, 1, 22), [0]) is True


def test_is_service_not_cancelled_entry_within_7_days():
    monday = date(2024, 1, 8)
    entry = make_entry(monday)
    service_levels = make_service_levels(entry)
    assert _is_service_cancelled_on_date(service_levels, monday, [0]) is False


# --- _get_service_levels_summary_dict ---


def test_get_summary_dict_active_service():
    monday = date(2024, 1, 8)
    levels = list(range(24))  # 0..23
    entry = make_entry(monday, service_levels=levels)
    service_levels = make_service_levels(entry)
    result = _get_service_levels_summary_dict(monday, service_levels, [0])
    assert result["cancelled"] is False
    assert result["tripsPerHour"] == levels
    assert result["totalTrips"] == round(sum(levels))


def test_get_summary_dict_cancelled_service():
    # Put entries far in the past so the 7-day check returns nothing
    old_monday = date(2024, 1, 1)
    entry = make_entry(old_monday)
    service_levels = make_service_levels(entry)
    result = _get_service_levels_summary_dict(date(2024, 2, 1), service_levels, [0])
    assert result["cancelled"] is True
    assert result["tripsPerHour"] is None
    assert result["totalTrips"] == 0


# --- summarize_weekly_service_around_date ---


def test_summarize_returns_weekday_saturday_sunday():
    # Create entries for Mon, Sat, Sun in the same week
    monday = date(2024, 1, 1)
    saturday = date(2024, 1, 6)
    sunday = date(2024, 1, 7)
    service_levels = make_service_levels(
        make_entry(monday),
        make_entry(saturday),
        make_entry(sunday),
    )
    result = summarize_weekly_service_around_date(monday, service_levels)
    assert "weekday" in result
    assert "saturday" in result
    assert "sunday" in result


def test_summarize_cancelled_saturday():
    # Only have weekday and Sunday entries, Saturday should be cancelled
    monday = date(2024, 1, 8)
    sunday = date(2024, 1, 7)
    service_levels = make_service_levels(
        make_entry(monday),
        make_entry(sunday),
    )
    # Look 2 weeks forward so no Saturday within 7 days
    result = summarize_weekly_service_around_date(date(2024, 1, 22), service_levels)
    assert result["saturday"]["cancelled"] is True


def test_summarize_not_cancelled_when_entry_exists():
    monday = date(2024, 1, 8)
    entry = make_entry(monday, service_levels=[3] * 24)
    service_levels = make_service_levels(entry)
    result = summarize_weekly_service_around_date(monday, service_levels)
    assert result["weekday"]["cancelled"] is False
    assert result["weekday"]["totalTrips"] == 72  # 3 * 24
