"""Scoreboard cells. Mirrors the dashboard's modules/home/compute/cells.ts."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from .config import METRICS, MIN_WEEKS
from .frequency import combine_coverage, goal_coverage
from .series import Point, Window, days_between, is_finite, js_round, mean, median, take_window, trend_of
from .status import status_for

# Cell fields that are always present, even when null. The rest are left out when unset.
REQUIRED_FIELDS = {"metric", "status", "score", "value", "unit", "trend"}

# Stands in for "not passed", where None is a meaningful value.
UNSET = object()


def compact(cell: dict) -> dict:
    return {key: value for key, value in cell.items() if key in REQUIRED_FIELDS or value is not None}


def month_year(day: str) -> str:
    return f"{date.fromisoformat(day[:10]):%b %Y}"


def month_day(day: str) -> str:
    d = date.fromisoformat(day[:10])
    return f"{d:%b} {d.day}"


def format_number(value: float) -> str:
    """1,234 / 123 / 12.3, the way the dashboard formats raw values in labels."""
    if value >= 1000:
        return f"{js_round(value):,}"
    if value >= 100:
        return str(js_round(value))
    return str(Decimal(value).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


@dataclass
class Baseline:
    value: float | None
    label: str | None = None
    # Why there's no baseline, when `value` is None.
    note: str | None = None


HELD_BACK_NOTES = {
    "noPrePandemicHistory": "No baseline yet: records start after 2020.",
    "seasonal": "No baseline yet: seasonal service.",
    "notComparable": "No baseline yet: service was counted differently before.",
}


def _missing_baseline_note(baselines: dict | None, series: list[dict | None]) -> str:
    held_back = next((s["heldBack"] for s in series if s and s.get("heldBack")), None)
    if held_back:
        reason = ((baselines or {}).get("heldBackReasons") or {}).get(held_back)
        return HELD_BACK_NOTES.get(held_back) or (f"No baseline yet. {reason}" if reason else "No baseline yet.")
    short = next((s for s in series if s and s.get("insufficientHistory") and s.get("historyWindow")), None)
    if baselines and short:
        start, end = short["historyWindow"]["start"], short["historyWindow"]["end"]
        years = baselines.get("lookbackYears") or 3
        if start > end:
            return (
                f"No baseline yet: records start {month_year(start)}, "
                f"and baselines only use weeks at least {years} years old."
            )
        if baselines.get("minHistoryWeeks") and short.get("historyWeeks") is not None:
            return (
                f"No baseline yet: {short['historyWeeks']} weeks of records are at least {years} years old, "
                f"and a baseline needs {baselines['minHistoryWeeks']}."
            )
    return "No baseline yet."


def baseline_for(baselines: dict | None, metric: str, series_ids: list[str]) -> Baseline:
    by_id = (((baselines or {}).get("metrics") or {}).get(metric) or {}).get("series") or {}
    series = [by_id.get(series_id) for series_id in series_ids]
    if not series or any(not s or s.get("heldBack") or not is_finite(s.get("value")) for s in series):
        return Baseline(value=None, note=_missing_baseline_note(baselines, series))
    window = series[0].get("bestWindow") if len(series_ids) == 1 else None
    return Baseline(
        value=sum(s["value"] for s in series),
        label=month_year(window["start"]) if window else None,
    )


def na_cell(metric: str, unit: str, note: str) -> dict:
    return {
        "metric": metric,
        "status": "na",
        "score": None,
        "value": None,
        "unit": unit,
        "trend": None,
        "notes": [note],
    }


def judged_cell(
    metric: str,
    score: float | None,
    *,
    unit: str,
    window: Window,
    now: datetime,
    value=UNSET,
    baseline_label: str | None = None,
    secondary: str | None = None,
    notes: list[str] | None = None,
    href: str | None = None,
    thresholds=None,
    direction: str | None = None,
    # The unit of `score` and `thresholds`, when the cell isn't judged like the rest of its column.
    score_unit: str | None = None,
    # Days to add to `as_of` before judging staleness (weekly points are dated by week start).
    as_of_offset_days: float = 6,
    min_points: int = MIN_WEEKS,
) -> dict:
    method = METRICS[metric]
    direction = direction or method["direction"]
    thresholds = list(thresholds or method["thresholds"])
    notes = list(notes or [])
    enough_data = len(window.values) >= min_points
    stale = not window.as_of or days_between(window.as_of, now) - as_of_offset_days > method["stale_days"]
    if window.excluded:
        weeks = f"{window.excluded} week{'s' if window.excluded > 1 else ''}"
        notes.append(f"Leaves out {weeks} of shutdowns or holidays.")
    if stale and window.as_of:
        notes.append(f"Latest data is from {month_day(window.as_of)}.")
    status = status_for(score, direction, thresholds) if enough_data and not stale else "insufficient"
    return compact(
        {
            "metric": metric,
            "status": status,
            "score": score,
            "value": score if value is UNSET else value,
            "unit": unit,
            "direction": direction,
            "thresholds": thresholds,
            "scoreUnit": score_unit or method["score_unit"],
            "baselineLabel": baseline_label,
            "trend": trend_of(window),
            "secondary": secondary,
            "spark": [v if is_finite(v) else None for _, v in window.points],
            "asOf": window.as_of,
            "notes": notes or None,
            "href": href,
        }
    )


def ratio_cell(
    metric: str,
    series: list[Point],
    baseline: Baseline,
    *,
    now: datetime,
    aggregate: str,
    unit: str,
    raw_unit_label: str,
    href: str | None = None,
    notes: list[str] | None = None,
    # Show the raw value (e.g. mph) as the headline instead of the ratio.
    headline_raw: bool = False,
) -> dict:
    """A weekly series judged against a historical best: `aggregate` over the window ÷ baseline."""
    window = take_window(series)
    raw = median(window.values) if aggregate == "median" else mean(window.values)
    raw_value = raw if is_finite(raw) else None
    if baseline.value is None:
        return {
            **judged_cell(metric, None, unit="count", window=window, now=now, href=href),
            "status": "insufficient",
            "value": raw_value,
            "unit": "count",
            "unitLabel": raw_unit_label,
            "notes": [
                *(notes or []),
                "No recent data." if raw_value is None else (baseline.note or "No baseline yet."),
            ],
        }
    score = raw_value / baseline.value if raw_value is not None else None
    return judged_cell(
        metric,
        score,
        unit=unit if headline_raw else "pct",
        value=raw_value if headline_raw else score,
        window=window,
        now=now,
        href=href,
        notes=notes,
        secondary=f"{format_number(raw_value)} {raw_unit_label}" if raw_value is not None else None,
        baseline_label=(
            f"Best: {format_number(baseline.value)} {raw_unit_label}"
            + (f" ({baseline.label})" if baseline.label else "")
        ),
    )


def trip_metrics_series(rows: list[dict] | None, pick) -> list[Point]:
    return [(row["date"], pick(row) if row.get("miles_covered") else None) for row in rows or []]


def mph(row: dict) -> float | None:
    return row["miles_covered"] / (row["total_time"] / 3600) if row.get("total_time") else None


def sum_histories(histories: list[dict[str, float] | None]) -> list[Point]:
    """Sum several lines' weekly histories by date, keeping only dates every line has."""
    present = [history for history in histories if history]
    if not present:
        return []
    dates = sorted(day for day in present[0] if all(day in history for history in present))
    return [(day, sum(history[day] for history in present)) for day in dates]


def frequency_cell(
    lines: list[tuple[dict, float | None]],
    goal_minutes: float,
    *,
    now: datetime,
    as_of: str,
    notes: list[str] | None = None,
    href: str | None = None,
) -> dict:
    current = combine_coverage([(goal_coverage(line, goal_minutes), weight) for line, weight in lines])
    year_ago = combine_coverage([(goal_coverage(line, goal_minutes, "oneYearAgo"), weight) for line, weight in lines])
    cell = judged_cell(
        "frequency",
        current,
        unit="pct",
        window=Window(values=[1], as_of=as_of),
        now=now,
        min_points=1,
        as_of_offset_days=0,
        href=href,
        notes=notes,
        secondary=f"Goal: every {goal_minutes} min",
        baseline_label=f"A year ago: {js_round(year_ago * 100)}% of hours" if year_ago is not None else None,
    )
    cell.pop("spark", None)
    return {
        **cell,
        "trend": current - year_ago if current is not None and year_ago is not None else None,
        "trendBasis": "yearAgo",
    }
