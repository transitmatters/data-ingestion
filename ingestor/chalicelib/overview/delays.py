"""Delays: the share of the service day with a delay alert up, from AlertDelaysDaily
(chalicelib/delays). Alert minutes there are already the union of overlapping alerts on a line, so
the share can't pass 100%.

The service day is first to last scheduled trip, from the same hourly schedule the Frequency column
uses. Rows with several lines pool them: total alert minutes over total service minutes.

Each cell also names the cause with the most alert minutes over the same weeks, from the per-cause
minutes the delays job splits each alert into.
"""

from datetime import date, datetime

from .. import constants
from ..delays.bus import BUS_AGGREGATE, BUS_ROUTE_GROUPS
from .cells import judged_cell, na_cell
from .frequency import service_span_hours
from .series import js_round, take_window, week_start

# Weeks pooled for the score.
RECENT_WEEKS = 4

DAY_TYPES = ["weekday"] * 5 + ["saturday", "sunday"]

# AlertDelaysDaily's alert minutes for each cause.
CAUSE_FIELDS = {cause: f"delay_alert_minutes_{cause}" for cause in constants.DELAY_BY_TYPE}
# Alerts that give no reason, never named as the top cause.
NO_REASON = "other"

# The AlertDelaysDaily line for each subway row.
SUBWAY_DELAY_LINES = {
    "line-red": "Red",
    "line-orange": "Orange",
    "line-blue": "Blue",
    # One row for all branches, so a trunk alert counts once.
    "line-green": "Green",
    "line-mattapan": "Mattapan",
}


def bus_delay_line(route_ids: list[str]) -> str | None:
    """The AlertDelaysDaily line for a bus line's routes: the route itself, or the combined row whose
    routes match. None when the delays job doesn't combine those routes."""
    if len(route_ids) == 1:
        return route_ids[0]
    group = next((g for g in BUS_ROUTE_GROUPS if set(g) == set(route_ids)), None)
    return ",".join(group) if group else None


def bus_delay_lines(lines: list[dict]) -> list[str]:
    """Every AlertDelaysDaily line a set of bus lines reads, plus the "Bus" row that marks which days
    have data."""
    keys = {BUS_AGGREGATE}
    for line in lines:
        route_ids = line.get("routeIds", [])
        key = bus_delay_line(route_ids)
        keys.update([key] if key else route_ids)
    return sorted(keys)


# Mattapan isn't in the schedule data on its own, so its service day is taken as 5 AM to 1 AM.
DEFAULT_SPAN_HOURS = 20


def _span_minutes(line: dict | None, day: str) -> float:
    if line is None:
        return DEFAULT_SPAN_HOURS * 60
    return service_span_hours(line, DAY_TYPES[date.fromisoformat(day).weekday()]) * 60


def _line_rows(line: dict | None, delays: dict[str, list[dict]], key: str | None) -> list[dict]:
    """The AlertDelaysDaily rows one line reads: its own, or each of its routes' when the delays job
    doesn't combine them."""
    if key:
        return delays.get(key, [])
    return [row for route in (line or {}).get("routeIds", []) for row in delays.get(route, [])]


def _line_minutes(line: dict | None, rows: list[dict], combined: bool) -> dict[str, float]:
    """Alert minutes per day for one line. Bus lines the delays job doesn't combine add up their
    routes, capped at the service day; a shared alert is then counted per route."""
    by_day: dict[str, float] = {}
    for row in rows:
        if "delay_alert_minutes" in row:
            by_day[row["date"]] = by_day.get(row["date"], 0) + row["delay_alert_minutes"]
    if combined:
        return by_day
    return {day: min(minutes, _span_minutes(line, day)) for day, minutes in by_day.items()}


def top_cause(rows: list[dict]) -> dict | None:
    """The cause with the most alert minutes over `rows`, leaving out alerts that give no reason, and
    its share of all the alert minutes. None when no alert gave a reason."""
    minutes = {cause: sum(row.get(field, 0) for row in rows) for cause, field in CAUSE_FIELDS.items()}
    cause = max((c for c in minutes if c != NO_REASON), key=minutes.__getitem__)
    if not minutes[cause]:
        return None
    return {"cause": cause, "share": round(minutes[cause] / sum(minutes.values()), 3)}


def _weekly_totals(
    lines: list[tuple[dict | None, dict[str, float]]], days: list[str], now: datetime
) -> list[tuple[str, dict[str, float]]]:
    """Alert and service minutes per complete Monday-to-Sunday week, oldest first. `days` are the
    days with alert data; a line with no row on one of them had no delays."""
    this_week = week_start(now.date().isoformat())
    weeks: dict[str, dict[str, float]] = {}
    for day in days:
        week = week_start(day)
        if week >= this_week:
            continue
        totals = weeks.setdefault(week, {"alert": 0, "service": 0, "days": 0})
        totals["days"] += 1
        for line, minutes in lines:
            span = _span_minutes(line, day)
            if span:
                totals["alert"] += minutes.get(day, 0)
                totals["service"] += span
    return sorted((week, totals) for week, totals in weeks.items() if totals["service"])


def delays_cell(
    lines: list[tuple[dict | None, dict[str, float]]],
    days: list[str],
    *,
    rows: list[dict],
    now: datetime,
    href: str | None = None,
    notes: list[str] | None = None,
) -> dict:
    """`rows` are every AlertDelaysDaily row the lines read, for the top cause."""
    weeks = _weekly_totals(lines, days, now)
    window = take_window([(week, totals["alert"] / totals["service"]) for week, totals in weeks], drop_low_weeks=False)
    recent = {"alert": 0, "service": 0, "days": 0}
    for _, totals in weeks[-RECENT_WEEKS:]:
        for field in recent:
            recent[field] += totals[field]
    if not recent["service"]:
        return judged_cell(
            "delays",
            None,
            unit="pct",
            window=window,
            now=now,
            href=href,
            notes=[*(notes or []), "No recent delay alert data."],
        )
    per_day = recent["alert"] / recent["days"] / len(lines)
    cell = judged_cell(
        "delays",
        recent["alert"] / recent["service"],
        unit="pct",
        window=window,
        now=now,
        min_points=RECENT_WEEKS,
        secondary=f"About {js_round(per_day)} min a day under a delay alert" + (" per route" if len(lines) > 1 else ""),
        notes=notes,
        href=href,
    )
    recent_weeks = {week for week, _ in weeks[-RECENT_WEEKS:]}
    with_data = set(days)
    cause = top_cause([row for row in rows if row["date"] in with_data and week_start(row["date"]) in recent_weeks])
    # Per route when several pool together, like the text above.
    return {**cell, "minutesPerDay": per_day, **({"topCause": cause} if cause else {})}


def subway_delays_cell(
    line_id: str, srd_line: dict | None, delays: dict[str, list[dict]] | None, *, now: datetime, href: str
) -> dict:
    if delays is None:
        return {**na_cell("delays", "pct", "Couldn't load delay alerts."), "status": "insufficient"}
    rows = _line_rows(srd_line, delays, SUBWAY_DELAY_LINES[line_id])
    minutes = _line_minutes(srd_line, rows, combined=True)
    notes = []
    if line_id == "line-green":
        notes.append("Alerts on any branch, counted once.")
    if srd_line is None:
        notes.append(f"Service day taken as {DEFAULT_SPAN_HOURS} hours.")
    return delays_cell([(srd_line, minutes)], sorted(minutes), rows=rows, now=now, href=href, notes=notes or None)


def bus_delays_cell(lines: list[dict], delays: dict[str, list[dict]] | None, *, now: datetime) -> dict:
    if delays is None:
        return {**na_cell("delays", "pct", "Couldn't load delay alerts."), "status": "insufficient"}
    days = sorted(row["date"] for row in delays.get(BUS_AGGREGATE, []) if "delay_alert_minutes" in row)
    lines_rows = []
    for line in lines:
        key = bus_delay_line(line.get("routeIds", []))
        lines_rows.append((line, _line_rows(line, delays, key), key is not None))
    return delays_cell(
        [(line, _line_minutes(line, rows, combined)) for line, rows, combined in lines_rows],
        days,
        rows=[row for _, rows, _ in lines_rows for row in rows],
        now=now,
        notes=["Each route's share of its service day, pooled."],
    )
