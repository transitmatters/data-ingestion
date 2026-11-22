"""Ridership against the same weeks a year earlier, and against the busiest stretch on record.

Shown on the home page but never judged: the T doesn't control ridership directly. Series are the
service & ridership dashboard's weekly medians of riders a day; each mode's source lags by a
different amount, so `asOf` says which weeks.
"""

from datetime import date, datetime, timedelta

from .cells import format_number, month_day, month_year, na_cell
from .config import METRICS, WINDOW_WEEKS
from .series import Point, days_between, is_finite, js_round, median

RECENT_WEEKS = 4
# Recent weeks that must have a twin a year earlier to compare against.
MIN_YEAR_AGO_WEEKS = 3
# A series starting after this misses the pre-pandemic peak, so its busiest stretch says so.
RECORDS_BEFORE_PANDEMIC = "2020-03-01"


def _year_before(week: str) -> str:
    return (date.fromisoformat(week) - timedelta(weeks=52)).isoformat()


def _week_end(week: str) -> str:
    return (date.fromisoformat(week) + timedelta(days=6)).isoformat()


def _busiest(series: list[Point]) -> tuple[str, float] | None:
    """The highest median of RECENT_WEEKS back-to-back weeks, and the week it starts."""
    best = None
    for i in range(len(series) - RECENT_WEEKS + 1):
        stretch = series[i : i + RECENT_WEEKS]
        if date.fromisoformat(stretch[-1][0]) - date.fromisoformat(stretch[0][0]) != timedelta(weeks=RECENT_WEEKS - 1):
            continue  # not back-to-back weeks
        value = median([v for _, v in stretch])
        if best is None or value > best[1]:
            best = (stretch[0][0], value)
    return best


def _insufficient(note: str, as_of: str | None = None) -> dict:
    cell = {**na_cell("ridership", "pct", note), "status": "insufficient"}
    return {**cell, "asOf": as_of} if as_of else cell


def ridership_cell(series: list[Point], *, now: datetime, href: str | None = None, method: dict | None = None) -> dict:
    """Median riders a day over the latest RECENT_WEEKS weeks against the same weeks a year earlier.
    The change compares only weeks that have a twin a year earlier, on both sides."""
    method = method or METRICS["ridership"]
    series = sorted((day, value) for day, value in series if is_finite(value) and value > 0)
    if len(series) < RECENT_WEEKS:
        return _insufficient("No recent ridership data.")
    recent = series[-RECENT_WEEKS:]
    as_of = recent[-1][0]
    if days_between(as_of, now) - 6 > method["stale_days"]:
        return _insufficient(f"Latest ridership data is from {month_year(as_of)}.", as_of)

    by_week = dict(series)
    pairs = [(value, by_week[_year_before(week)]) for week, value in recent if _year_before(week) in by_week]
    if len(pairs) < MIN_YEAR_AGO_WEEKS:
        return _insufficient("No ridership from a year earlier to compare with.", as_of)

    now_riders = median([value for _, value in recent])
    busiest = _busiest(series)
    return {
        "metric": "ridership",
        "status": "unscored",
        "score": None,
        "value": median([now for now, _ in pairs]) / median([then for _, then in pairs]) - 1,
        "unit": "pct",
        "trend": None,
        "secondary": f"{format_number(now_riders)} riders a day",
        "baselineLabel": (
            f"{js_round(now_riders / busiest[1] * 100)}% of its busiest stretch ({month_year(busiest[0])})"
            + (f", in records from {month_year(series[0][0])}" if series[0][0] > RECORDS_BEFORE_PANDEMIC else "")
            if busiest
            else None
        ),
        "spark": [value for _, value in series[-WINDOW_WEEKS:]],
        "asOf": as_of,
        "notes": [f"The {RECENT_WEEKS} weeks to {month_day(_week_end(as_of))}, against the same weeks a year earlier."],
        "href": href,
    }
