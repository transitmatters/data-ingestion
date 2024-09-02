from typing import TypeVar, Callable, Optional
from datetime import date, timedelta

from .util import date_range, date_to_string
from .config import FILL_DATE_RANGES
from .types import WeeklyMedianTimeSeries

Entry = TypeVar("Entry")
EntryDict = dict[date, Entry]


def _iterate_mondays(
    entries: EntryDict,
    start_date: date,
    max_end_date: date,
):
    max_found_date = max(entries.keys())
    end_date = min(max_end_date, max_found_date)
    for today in date_range(start_date, end_date):
        if today in entries:
            yield today, entries[today]


def _get_entry_value_for_date(
    entries: EntryDict,
    date: date,
    entry_value_getter: Callable[[Entry], float],
) -> Optional[float]:
    if date in entries:
        return entry_value_getter(entries[date])
    for previous_date in sorted(entries.keys(), reverse=True):
        if previous_date < date:
            return entry_value_getter(entries[previous_date])
    return None


def _choose_between_previous_and_current_value(
    current_value: Optional[float],
    previous_value: Optional[float],
    today: date,
) -> float:
    if current_value is None and previous_value is not None:
        return previous_value
    is_weekend = today.weekday() >= 5
    if is_weekend and previous_value:
        return previous_value
    return current_value or 0


def _date_ranges_intersect(
    a: tuple[date, date],
    b: tuple[date, date],
) -> bool:
    return a[0] <= b[1] and b[0] <= a[1]


def _find_zero_ranges_in_time_series(time_series: list[float]) -> list[tuple[int, int]]:
    zero_ranges = []
    current_range_start = None
    for i, value in enumerate(time_series):
        if value == 0:
            if current_range_start is None:
                current_range_start = i
        else:
            if current_range_start is not None:
                zero_ranges.append((current_range_start, i - 1))
                current_range_start = None
    if current_range_start is not None:
        zero_ranges.append((current_range_start, len(time_series) - 1))
    return zero_ranges


def _fill_zero_ranges_in_time_series(time_series: list[float], start_date: date) -> list[float]:
    zero_ranges = _find_zero_ranges_in_time_series(time_series)
    altered_time_series = time_series.copy()
    for range_start_idx, range_end_idx in zero_ranges:
        last_non_zero_value = time_series[range_start_idx - 1] if range_start_idx > 0 else 0
        zero_date_range = (
            start_date + timedelta(days=range_start_idx),
            start_date + timedelta(days=range_end_idx),
        )
        should_fill_special_range = any(
            _date_ranges_intersect(zero_date_range, fill_range) for fill_range in FILL_DATE_RANGES
        )
        should_fill_small_range = range_end_idx - range_start_idx <= 5
        should_fill = should_fill_special_range or should_fill_small_range
        if should_fill:
            for i in range(range_start_idx, range_end_idx + 1):
                altered_time_series[i] = last_non_zero_value
    return altered_time_series


def _get_monday_of_week_containing_date(date: date) -> date:
    return date - timedelta(days=date.weekday())


def _bucket_by_week(entries: dict[date, Entry]) -> dict[date, list[Entry]]:
    buckets: dict[date, list[Entry]] = {}
    for today, entry in entries.items():
        week_start = _get_monday_of_week_containing_date(today)
        buckets.setdefault(week_start, [])
        buckets[week_start].append(entry)
    return buckets


def get_weekly_median_time_series(
    entries: dict[date, Entry],
    entry_value_getter: Callable[[Entry], float],
    start_date: date,
    max_end_date: date,
) -> WeeklyMedianTimeSeries:
    weekly_buckets = _bucket_by_week(entries)
    weekly_medians: dict[str, float] = {}
    for week_start, week_entries in _iterate_mondays(weekly_buckets, start_date, max_end_date):
        week_values = [entry_value_getter(entry) for entry in week_entries]
        week_values.sort()
        weekly_medians[date_to_string(week_start)] = week_values[len(week_values) // 2]
    return weekly_medians


def merge_weekly_median_time_series(many_series: list[WeeklyMedianTimeSeries]) -> WeeklyMedianTimeSeries:
    merged_series: dict[str, float] = {}
    for series in many_series:
        for week_start, value in series.items():
            merged_series.setdefault(week_start, 0)
            merged_series[week_start] += value
    return merged_series


def get_weekly_median_time_series_entry_for_date(series: WeeklyMedianTimeSeries, date: date) -> Optional[float]:
    monday = _get_monday_of_week_containing_date(date)
    return series.get(date_to_string(monday))


def get_latest_weekly_median_time_series_entry(series: WeeklyMedianTimeSeries) -> Optional[float]:
    latest_date = max(series.keys())
    return series.get(latest_date)


def get_earliest_weekly_median_time_series_entry(series: WeeklyMedianTimeSeries) -> Optional[float]:
    earliest_date = min(series.keys())
    return series.get(earliest_date)
