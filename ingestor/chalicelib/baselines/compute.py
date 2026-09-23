"""Pure functions for computing "historical best" baselines.

A historical best is the highest rolling 12-week median of a weekly series, using only
weeks that ended at least ``LOOKBACK_YEARS`` before today. Recent performance can't move
the goalposts until it is that old, and a single outlier week can't set the bar.
"""

import math
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import median
from typing import Iterable

WINDOW_WEEKS = 12
MIN_WEEKS_IN_WINDOW = 9
LOOKBACK_YEARS = 3
MIN_HISTORY_WEEKS = 52
PRE_PANDEMIC_DATE = date(2020, 3, 1)


@dataclass(frozen=True)
class Best:
    value: float
    window_start: date
    window_end: date
    weeks_with_data: int


def week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def cutoff_date(today: date, years: int = LOOKBACK_YEARS) -> date:
    try:
        return today.replace(year=today.year - years)
    except ValueError:
        # Feb 29 -> Feb 28
        return today.replace(year=today.year - years, day=28)


def to_weekly(points: Iterable[tuple[date, float | None]]) -> dict[date, float]:
    """Bucket (date, value) points into Monday-start weeks.

    If a week has a point dated on its Monday (the canonical weekly record), that point is used and
    stray mid-week records are ignored: the Ridership table has ~30-40 such records per bus route per
    year that run up to 2x the route's normal level. Otherwise points in the week are averaged.

    Missing, non-finite and non-positive values are dropped: a zero here means "no service/data"
    (shutdowns, seasonal ferry gaps), never a real measurement to compare against.
    """
    buckets: dict[date, list[tuple[date, float]]] = {}
    for d, value in points:
        if value is None:
            continue
        value = float(value)
        if not math.isfinite(value) or value <= 0:
            continue
        buckets.setdefault(week_start(d), []).append((d, value))

    weekly = {}
    for week, entries in sorted(buckets.items()):
        canonical = [value for d, value in entries if d == week]
        values = canonical or [value for _, value in entries]
        weekly[week] = sum(values) / len(values)
    return weekly


def complete_weeks_before(weekly: dict[date, float], cutoff: date) -> dict[date, float]:
    """Weeks that ended on or before the cutoff."""
    return {week: value for week, value in weekly.items() if week + timedelta(days=6) <= cutoff}


def best_rolling_median(
    weekly: dict[date, float],
    cutoff: date,
    window_weeks: int = WINDOW_WEEKS,
    min_weeks: int = MIN_WEEKS_IN_WINDOW,
) -> Best | None:
    """Highest median over any ``window_weeks`` consecutive calendar weeks ending by ``cutoff``.

    Windows are calendar-based, so a window never bridges a long data gap; windows with fewer
    than ``min_weeks`` weeks of data are skipped. Ties keep the earliest window.
    """
    eligible = complete_weeks_before(weekly, cutoff)
    if not eligible:
        return None

    first_week = min(eligible)
    last_week = max(eligible)
    best: Best | None = None
    window_end = first_week + timedelta(weeks=window_weeks - 1)
    while window_end <= last_week:
        window_start = window_end - timedelta(weeks=window_weeks - 1)
        values = [
            eligible[week]
            for week in (window_start + timedelta(weeks=i) for i in range(window_weeks))
            if week in eligible
        ]
        if len(values) >= min_weeks:
            value = median(values)
            if best is None or value > best.value:
                best = Best(
                    value=value,
                    window_start=window_start,
                    window_end=window_end + timedelta(days=6),
                    weeks_with_data=len(values),
                )
        window_end += timedelta(weeks=1)
    return best


def summarize_series(weekly: dict[date, float], cutoff: date, decimals: int) -> dict:
    """Baseline entry for one series, suitable for JSON output.

    Series with less than ``MIN_HISTORY_WEEKS`` of data before the cutoff get ``value: None`` and
    ``insufficientHistory: True`` so consumers can't accidentally judge against a thin baseline.
    """
    eligible = complete_weeks_before(weekly, cutoff)
    history_start = min(weekly).isoformat() if weekly else None
    entry = {
        "value": None,
        "bestWindow": None,
        "historyWindow": {"start": history_start, "end": cutoff.isoformat()},
        "historyWeeks": len(eligible),
        "prePandemicHistory": bool(eligible) and min(eligible) < PRE_PANDEMIC_DATE,
        "insufficientHistory": len(eligible) < MIN_HISTORY_WEEKS,
    }
    if entry["insufficientHistory"]:
        return entry

    best = best_rolling_median(weekly, cutoff)
    if best is None:
        entry["insufficientHistory"] = True
        return entry

    entry["value"] = round(best.value, decimals) if decimals > 0 else int(round(best.value))
    entry["bestWindow"] = {
        "start": best.window_start.isoformat(),
        "end": best.window_end.isoformat(),
        "weeksWithData": best.weeks_with_data,
    }
    return entry
