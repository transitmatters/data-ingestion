from datetime import date, timedelta
from typing import Callable, Optional, TypeVar

from .config import FILL_DATE_RANGES
from .types import WeeklyMedianTimeSeries
from .util import date_range, date_to_string

Entry = TypeVar("Entry")
EntryDict = dict[date, Entry]


def _iterate_mondays(
    entries: EntryDict,
    start_date: date,
    max_end_date: date,
):
    """Yield date-entry pairs for each date present in entries within the given range.

    Args:
        entries: A dictionary mapping dates to entry values.
        start_date: The start date of the iteration range.
        max_end_date: The maximum end date; iteration stops at the earlier of this or the max entry date.

    Yields:
        Tuples of (date, entry) for each date found in entries within the range.
    """
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
    """Get the entry value for a specific date, falling back to the most recent prior date.

    Args:
        entries: A dictionary mapping dates to entry values.
        date: The target date to look up.
        entry_value_getter: A callable that extracts a float value from an entry.

    Returns:
        The float value for the date, the most recent prior date's value, or None if no prior entry exists.
    """
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
    """Select between the current and previous value, preferring the previous value on weekends.

    Args:
        current_value: The value for the current date, or None.
        previous_value: The value from a previous date, or None.
        today: The current date, used to check if it is a weekend.

    Returns:
        The selected float value, defaulting to 0 if both are None.
    """
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
    """Check whether two date ranges overlap.

    Args:
        a: The first date range as a (start, end) tuple.
        b: The second date range as a (start, end) tuple.

    Returns:
        True if the two date ranges intersect.
    """
    return a[0] <= b[1] and b[0] <= a[1]


def _find_zero_ranges_in_time_series(time_series: list[float]) -> list[tuple[int, int]]:
    """Identify contiguous ranges of zero values in a time series.

    Args:
        time_series: A list of float values representing a time series.

    Returns:
        A list of (start_index, end_index) tuples for each contiguous range of zeros.
    """
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
    """Fill small or holiday-related zero gaps in a time series with the last non-zero value.

    Args:
        time_series: A list of float values representing a time series.
        start_date: The date corresponding to the first element of the time series.

    Returns:
        A copy of the time series with qualifying zero ranges filled in.
    """
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
    """Get the Monday of the week containing the given date.

    Args:
        date: The date to find the Monday for.

    Returns:
        The date of the Monday of that week.
    """
    return date - timedelta(days=date.weekday())


def _bucket_by_week(entries: dict[date, Entry]) -> dict[date, list[Entry]]:
    """Group entries by the Monday of the week they belong to.

    Args:
        entries: A dictionary mapping dates to entry values.

    Returns:
        A dictionary mapping Monday dates to lists of entries from that week.
    """
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
    """Compute a weekly median time series from daily entries.

    Args:
        entries: A dictionary mapping dates to entry values.
        entry_value_getter: A callable that extracts a float value from an entry.
        start_date: The start date of the time series.
        max_end_date: The maximum end date of the time series.

    Returns:
        A dictionary mapping date strings (yyyy-mm-dd) to weekly median values.
    """
    weekly_buckets = _bucket_by_week(entries)
    weekly_medians: dict[str, float] = {}
    for week_start, week_entries in _iterate_mondays(weekly_buckets, start_date, max_end_date):
        week_values = [entry_value_getter(entry) for entry in week_entries]
        week_values.sort()
        weekly_medians[date_to_string(week_start)] = week_values[len(week_values) // 2]
    return weekly_medians


def merge_weekly_median_time_series(many_series: list[WeeklyMedianTimeSeries]) -> WeeklyMedianTimeSeries:
    """Merge multiple weekly median time series by summing values for each week.

    Args:
        many_series: A list of WeeklyMedianTimeSeries dictionaries to merge.

    Returns:
        A single WeeklyMedianTimeSeries with summed values for each week.
    """
    merged_series: dict[str, float] = {}
    for series in many_series:
        for week_start, value in series.items():
            merged_series.setdefault(week_start, 0)
            merged_series[week_start] += value
    return merged_series


def get_weekly_median_time_series_entry_for_date(series: WeeklyMedianTimeSeries, date: date) -> Optional[float]:
    """Look up the weekly median value for the week containing a given date.

    Args:
        series: A WeeklyMedianTimeSeries dictionary.
        date: The date to look up.

    Returns:
        The median value for that week, or None if not found.
    """
    monday = _get_monday_of_week_containing_date(date)
    return series.get(date_to_string(monday))


def get_latest_weekly_median_time_series_entry(series: WeeklyMedianTimeSeries) -> Optional[float]:
    """Get the value of the most recent week in a weekly median time series.

    Args:
        series: A WeeklyMedianTimeSeries dictionary.

    Returns:
        The value for the latest week, or None if not found.
    """
    latest_date = max(series.keys())
    return series.get(latest_date)


def get_earliest_weekly_median_time_series_entry(series: WeeklyMedianTimeSeries) -> Optional[float]:
    """Get the value of the earliest week in a weekly median time series.

    Args:
        series: A WeeklyMedianTimeSeries dictionary.

    Returns:
        The value for the earliest week, or None if not found.
    """
    earliest_date = min(series.keys())
    return series.get(earliest_date)
