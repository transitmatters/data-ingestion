"""Weekly series helpers. Mirrors the dashboard's modules/home/compute/series.ts."""

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from .config import WINDOW_WEEKS

# (date "YYYY-MM-DD", value or None)
Point = tuple[str, float | None]

NAN = float("nan")


def is_finite(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else NAN


def median(values: list[float]) -> float:
    if not values:
        return NAN
    ordered = sorted(values)
    mid = len(ordered) // 2
    return ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2


def js_round(value: float) -> int:
    """Math.round: halves round up, unlike Python's round()."""
    return math.floor(value + 0.5)


def from_record(record: dict[str, float] | None) -> list[Point]:
    return sorted((record or {}).items())


@dataclass
class Window:
    points: list[Point] = field(default_factory=list)
    values: list[float] = field(default_factory=list)
    # Weeks dropped as planned work (a shutdown or diversion), not counted against the line.
    excluded: int = 0
    as_of: str | None = None


def take_window(series: list[Point], drop_low_weeks: bool = True) -> Window:
    """The last WINDOW_WEEKS points, without gaps and with up to 3 planned-work weeks (under 60% of
    the window median) left out. Those weeks show a shutdown, not how the line normally runs."""
    points = series[-WINDOW_WEEKS:]
    present = [(i, value) for i, (_, value) in enumerate(points) if is_finite(value)]
    mid = median([value for _, value in present])
    low: set[int] = set()
    if drop_low_weeks:
        candidates = sorted((p for p in present if p[1] < 0.6 * mid), key=lambda p: p[1])
        low = {i for i, _ in candidates[:3]}
    return Window(
        points=points,
        values=[value for i, value in present if i not in low],
        excluded=len(low),
        as_of=points[present[-1][0]][0] if present else None,
    )


def trend_of(window: Window) -> float | None:
    """Last 2 weeks vs the whole window, as a fraction."""
    recent = window.values[-2:]
    if len(recent) < 2 or len(window.values) < 4:
        return None
    base = mean(window.values)
    return mean(recent) / base - 1 if base else None


def days_between(start: str, now: datetime) -> float:
    start_at = datetime.combine(date.fromisoformat(start[:10]), datetime.min.time(), timezone.utc)
    return (now - start_at).total_seconds() / 86_400
