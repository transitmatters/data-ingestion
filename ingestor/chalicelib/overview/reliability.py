"""Reliability: the share of headways within 75-125% of the scheduled headway.

Subway rows come from HeadwayReliability (chalicelib/headway_reliability), bus rows from
DeliveredTripMetricsBus (mbta-performance). Both are daily bucket counts with the same field names,
so routes and days pool by summing before dividing.

Commuter Rail runs to a timetable, so its row uses the MBTA's published on-time counts instead
(the Reliability table, chalicelib/reliability/commuter_rail.py).
"""

from datetime import datetime

from ..headway_reliability.buckets import HEADWAY_FIELDS
from .bus_fleet import week_start
from .cells import judged_cell, month_day, na_cell
from .config import CR_RELIABILITY
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


CR_FIELDS = ["otpNumerator", "otpDenominator", "cancelled"]


def _cr_weekly_totals(rows: list[dict]) -> list[tuple[str, dict[str, float]]]:
    """On-time counts summed per Monday-to-Sunday week, oldest first. The MBTA publishes in batches,
    so the newest week is dropped if the batch ends partway through it."""
    weeks: dict[str, dict[str, float]] = {}
    days: dict[str, set[str]] = {}
    for row in rows:
        week = week_start(row["date"])
        totals = weeks.setdefault(week, dict.fromkeys(CR_FIELDS, 0))
        days.setdefault(week, set()).add(row["date"])
        for field in CR_FIELDS:
            totals[field] += row.get(field) or 0
    complete = sorted((week, totals) for week, totals in weeks.items() if totals["otpDenominator"])
    if complete and len(days[complete[-1][0]]) < 7:
        complete.pop()
    return complete


def cr_reliability_cell(rows: list[dict] | None, *, now: datetime, href: str | None = None) -> dict:
    """Daily rows from every Commuter Rail line, pooled. `rows` is None when they couldn't be loaded."""
    if rows is None:
        return {**na_cell("reliability", "pct", "Couldn't load on-time data."), "status": "insufficient"}

    weeks = _cr_weekly_totals(rows)
    window = take_window(
        [(week, totals["otpNumerator"] / totals["otpDenominator"]) for week, totals in weeks], drop_low_weeks=False
    )
    recent = dict.fromkeys(CR_FIELDS, 0)
    for _, totals in weeks[-RECENT_WEEKS:]:
        for field in CR_FIELDS:
            recent[field] += totals[field]
    peak = {field: 0 for field in ("otpNumerator", "otpDenominator")}
    recent_weeks = {week for week, _ in weeks[-RECENT_WEEKS:]}
    for row in rows:
        if week_start(row["date"]) in recent_weeks:
            for field in peak:
                peak[field] += (row.get("peak") or {}).get(field) or 0

    if not recent["otpDenominator"]:
        return judged_cell(
            "reliability",
            None,
            unit="pct",
            window=window,
            now=now,
            method=CR_RELIABILITY,
            href=href,
            notes=["No recent on-time data."],
        )
    cancelled = int(recent["cancelled"])
    return judged_cell(
        "reliability",
        recent["otpNumerator"] / recent["otpDenominator"],
        unit="pct",
        window=window,
        now=now,
        min_points=RECENT_WEEKS,
        method=CR_RELIABILITY,
        secondary=(
            f"{js_round(peak['otpNumerator'] / peak['otpDenominator'] * 100)}% on time at peak"
            if peak["otpDenominator"]
            else None
        ),
        baseline_label=f"{cancelled} train{'' if cancelled == 1 else 's'} cancelled",
        notes=[
            "Trains on time by the MBTA's measure, from its published counts.",
            f"The MBTA publishes these in batches; the latest is from {month_day(max(row['date'] for row in rows))}.",
        ],
        href=href,
    )
