"""Reliability: the share of headways within 75-125% of the scheduled headway.

Subway rows come from HeadwayReliability (chalicelib/headway_reliability), bus rows from
DeliveredTripMetricsBus (mbta-performance). Both are daily bucket counts with the same field names,
so routes and days pool by summing before dividing.
"""

from datetime import datetime

from ..headway_reliability.buckets import HEADWAY_FIELDS
from .bus_fleet import week_start
from .cells import judged_cell, na_cell
from .series import js_round, take_window

# Weeks pooled for the score.
RECENT_WEEKS = 4

WITHIN_75_125 = ["headways_75_125"]
WITHIN_50_150 = ["headways_50_75", "headways_75_125", "headways_125_150"]
BUNCHED = ["headways_under_50"]


def _weekly_totals(rows: list[dict], now: datetime) -> list[tuple[str, dict[str, float]]]:
    """Counts summed per complete Monday-to-Sunday week, oldest first."""
    this_week = week_start(now.date().isoformat())
    weeks: dict[str, dict[str, float]] = {}
    for row in rows:
        week = week_start(row["date"])
        if week >= this_week:
            continue
        totals = weeks.setdefault(week, dict.fromkeys(HEADWAY_FIELDS, 0))
        for field in HEADWAY_FIELDS:
            totals[field] += row.get(field) or 0
    return sorted((week, totals) for week, totals in weeks.items() if sum(totals.values()))


def _share(totals: dict[str, float], fields: list[str]) -> float | None:
    total = sum(totals[f] for f in HEADWAY_FIELDS)
    return sum(totals[f] for f in fields) / total if total else None


def reliability_cell(
    rows: list[dict] | None, *, now: datetime, href: str | None = None, notes: list[str] | None = None
) -> dict:
    """Daily rows from any number of lines or routes, pooled. `rows` is None when they couldn't be
    loaded."""
    if rows is None:
        return {**na_cell("reliability", "pct", "Couldn't load headway data."), "status": "insufficient"}

    weeks = _weekly_totals(rows, now)
    window = take_window([(week, _share(totals, WITHIN_75_125)) for week, totals in weeks], drop_low_weeks=False)
    recent = dict.fromkeys(HEADWAY_FIELDS, 0)
    for _, totals in weeks[-RECENT_WEEKS:]:
        for field in HEADWAY_FIELDS:
            recent[field] += totals[field]

    def pct(fields: list[str]) -> str:
        return f"{js_round((_share(recent, fields) or 0) * 100)}%"

    has_recent = bool(sum(recent.values()))
    return judged_cell(
        "reliability",
        _share(recent, WITHIN_75_125) if has_recent else None,
        unit="pct",
        window=window,
        now=now,
        min_points=RECENT_WEEKS,
        secondary=f"{pct(WITHIN_50_150)} within 50–150% of schedule" if has_recent else None,
        baseline_label=f"{pct(BUNCHED)} bunched (half the scheduled gap or less)" if has_recent else None,
        notes=(notes or []) if has_recent else [*(notes or []), "No recent headway data."],
        href=href,
    )
