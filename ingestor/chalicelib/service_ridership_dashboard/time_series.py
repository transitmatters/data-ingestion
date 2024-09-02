from typing import TypeVar, Callable, Optional, Dict
from datetime import date, timedelta

from .util import date_range
from .config import FILL_DATE_RANGES

Entry = TypeVar("Entry")
EntryDict = dict[date, Entry]


def _valid_date_range(
    entries: EntryDict,
    start_date: date,
    max_end_date: date,
):
    max_found_date = max(entries.keys())
    end_date = min(max_end_date, max_found_date)
    yield from date_range(start_date, end_date)


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


def get_time_series(
    entries: dict[date, Entry],
    entry_value_getter: Callable[[Entry], float],
    start_date: date,
    max_end_date: date,
) -> list[float]:
    time_series: list[float] = []
    for today in _valid_date_range(entries, start_date, max_end_date):
        current_value = _get_entry_value_for_date(entries, today, entry_value_getter)
        previous_value = time_series[-1] if time_series else None
        chosen_value = _choose_between_previous_and_current_value(
            current_value=current_value,
            previous_value=previous_value,
            today=today,
        )
        time_series.append(chosen_value)
    return _fill_zero_ranges_in_time_series(time_series=time_series, start_date=start_date)
