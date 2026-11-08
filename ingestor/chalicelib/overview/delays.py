"""Delays: the share of the service day with a delay alert up, from AlertDelaysDaily
(chalicelib/delays). Alert minutes there are already the union of overlapping alerts on a line, so
the share can't pass 100%.

The service day is first to last scheduled trip, from the same hourly schedule the Frequency column
uses. Rows with several lines pool them: total alert minutes over total service minutes.
"""

from datetime import date, datetime

from ..delays.bus import BUS_AGGREGATE, BUS_ROUTE_GROUPS
from .bus_fleet import week_start
from .cells import judged_cell, na_cell
from .frequency import service_span_hours
from .series import js_round, take_window

# Weeks pooled for the score.
RECENT_WEEKS = 4

DAY_TYPES = ["weekday"] * 5 + ["saturday", "sunday"]

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


def _line_minutes(line: dict, delays: dict[str, list[dict]], key: str | None) -> dict[str, float]:
    """Alert minutes per day for one line. Bus lines the delays job doesn't combine add up their
    routes, capped at the service day; a shared alert is then counted per route."""
    if key:
        return {row["date"]: row["delay_alert_minutes"] for row in delays.get(key, []) if "delay_alert_minutes" in row}
    by_day: dict[str, float] = {}
    for route in line.get("routeIds", []):
        for row in delays.get(route, []):
            if "delay_alert_minutes" in row:
                by_day[row["date"]] = by_day.get(row["date"], 0) + row["delay_alert_minutes"]
    return {day: min(minutes, _span_minutes(line, day)) for day, minutes in by_day.items()}


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
    now: datetime,
    href: str | None = None,
    notes: list[str] | None = None,
) -> dict:
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
    return judged_cell(
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


def subway_delays_cell(
    line_id: str, srd_line: dict | None, delays: dict[str, list[dict]] | None, *, now: datetime, href: str
) -> dict:
    if delays is None:
        return {**na_cell("delays", "pct", "Couldn't load delay alerts."), "status": "insufficient"}
    minutes = _line_minutes(srd_line, delays, SUBWAY_DELAY_LINES[line_id])
    notes = []
    if line_id == "line-green":
        notes.append("Alerts on any branch, counted once.")
    if srd_line is None:
        notes.append(f"Service day taken as {DEFAULT_SPAN_HOURS} hours.")
    return delays_cell([(srd_line, minutes)], sorted(minutes), now=now, href=href, notes=notes or None)


def bus_delays_cell(lines: list[dict], delays: dict[str, list[dict]] | None, *, now: datetime) -> dict:
    if delays is None:
        return {**na_cell("delays", "pct", "Couldn't load delay alerts."), "status": "insufficient"}
    days = sorted(row["date"] for row in delays.get(BUS_AGGREGATE, []) if "delay_alert_minutes" in row)
    return delays_cell(
        [(line, _line_minutes(line, delays, bus_delay_line(line.get("routeIds", [])))) for line in lines],
        days,
        now=now,
        notes=["Each route's share of its service day, pooled."],
    )
