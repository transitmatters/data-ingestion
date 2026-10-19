"""Pure functions for computing "historical best" baselines.

A historical best is the highest rolling-window average of a weekly series, using only weeks
that ended at least ``LOOKBACK_YEARS`` before today. Recent performance can't move the
goalposts until it is that old, and averaging over a window keeps one outlier week from
setting the bar on its own.
"""

import math
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import mean, median
from typing import Iterable, Literal

LOOKBACK_YEARS = 3
MIN_HISTORY_WEEKS = 52
PRE_PANDEMIC_DATE = date(2020, 3, 1)


@dataclass(frozen=True)
class Window:
    weeks: int
    min_weeks: int
    aggregate: Literal["mean", "median"]

    def apply(self, values: list[float]) -> float:
        return mean(values) if self.aggregate == "mean" else median(values)

    def to_json(self) -> dict:
        return {"weeks": self.weeks, "minWeeks": self.min_weeks, "aggregate": self.aggregate}


# Best sustained month: close to what a line has proven it can do, without trusting a single week.
SUSTAINED_PEAK = Window(weeks=4, min_weeks=3, aggregate="mean")
# Schedules are flat for months at a time, and the weekly history has artifact weeks (e.g. overlapping
# GTFS feeds the week of 2023-05-01), so a longer median reproduces the real scheduled level.
STABLE_LEVEL = Window(weeks=12, min_weeks=9, aggregate="median")


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


def best_rolling(weekly: dict[date, float], cutoff: date, window: Window) -> Best | None:
    """Highest ``window`` aggregate over any run of consecutive calendar weeks ending by ``cutoff``.

    Windows are calendar-based, so a window never bridges a long data gap; windows with fewer
    than ``window.min_weeks`` weeks of data are skipped. Ties keep the earliest window.
    """
    eligible = complete_weeks_before(weekly, cutoff)
    if not eligible:
        return None

    first_week = min(eligible)
    last_week = max(eligible)
    best: Best | None = None
    window_end = first_week + timedelta(weeks=window.weeks - 1)
    while window_end <= last_week:
        window_start = window_end - timedelta(weeks=window.weeks - 1)
        values = [
            eligible[week]
            for week in (window_start + timedelta(weeks=i) for i in range(window.weeks))
            if week in eligible
        ]
        if len(values) >= window.min_weeks:
            value = window.apply(values)
            if best is None or value > best.value:
                best = Best(
                    value=value,
                    window_start=window_start,
                    window_end=window_end + timedelta(days=6),
                    weeks_with_data=len(values),
                )
        window_end += timedelta(weeks=1)
    return best


def summarize_series(
    weekly: dict[date, float],
    cutoff: date,
    decimals: int,
    window: Window,
    held_back_reason: str | None = None,
) -> dict:
    """Baseline entry for one series, suitable for JSON output.

    Series with less than ``MIN_HISTORY_WEEKS`` of data before the cutoff get ``value: None`` and
    ``insufficientHistory: True`` so consumers can't accidentally judge against a thin baseline.

    A ``held_back_reason`` publishes ``value: None`` with the computed number moved to
    ``candidateValue``, for series whose baseline isn't trusted yet; consumers keep their own
    value (or show no baseline) until it's resolved.
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

    best = best_rolling(weekly, cutoff, window)
    if best is None:
        entry["insufficientHistory"] = True
        return entry

    value = round(best.value, decimals) if decimals > 0 else int(round(best.value))
    entry["bestWindow"] = {
        "start": best.window_start.isoformat(),
        "end": best.window_end.isoformat(),
        "weeksWithData": best.weeks_with_data,
    }
    if held_back_reason:
        entry["candidateValue"] = value
        entry["heldBack"] = held_back_reason
    else:
        entry["value"] = value
    return entry
