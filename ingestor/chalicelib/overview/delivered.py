"""Share of scheduled service delivered, shown next to the Service cell's trips per day.

Subway compares delivered and scheduled service hours, the same way the dashboard's Service page
does (server/chalicelib/service_hours.py): delivered hours are each direction's trips times its
mean trip time, from DeliveredTripMetricsExtended; scheduled hours are ScheduledServiceDaily's
serviceMinutes. Bus counts scheduled trips that LAMP never saw a vehicle for (mbta-performance's
DeliveredTripMetricsBus rows).
"""

from datetime import datetime

from .bus_fleet import week_start
from .series import js_round

# Weeks pooled, like the other cells' scores.
RECENT_WEEKS = 4

# DeliveredTripMetricsExtended routes and the ScheduledServiceDaily route for each subway line.
SUBWAY_ROUTES = {
    "line-red": (["line-red", "line-red-a", "line-red-b"], "Red"),
    "line-orange": (["line-orange"], "Orange"),
    "line-blue": (["line-blue"], "Blue"),
    "line-green": (["line-green-b", "line-green-c", "line-green-d", "line-green-e"], "Green"),
    "line-mattapan": (["line-mattapan"], "Mattapan"),
}

EXTENDED_FIELDS = ["dir_0_exclusive_count", "dir_1_exclusive_count", "dir_0_inclusive_mean", "dir_1_inclusive_mean"]
SCHEDULED_FIELDS = ["serviceMinutes"]
BUS_TRIP_FIELDS = ["scheduled_trips", "unobserved_trips"]


def _recent_days(days: set[str], now: datetime) -> set[str]:
    """`days` within the last RECENT_WEEKS complete Monday-to-Sunday weeks that have any."""
    this_week = week_start(now.date().isoformat())
    weeks = sorted({week_start(day) for day in days if week_start(day) < this_week})[-RECENT_WEEKS:]
    return {day for day in days if week_start(day) in weeks}


def _delivered_hours(row: dict) -> float:
    return (
        sum((row.get(f"dir_{d}_exclusive_count") or 0) * (row.get(f"dir_{d}_inclusive_mean") or 0) for d in (0, 1))
        / 3600
    )


def subway_delivered_share(extended: list[dict] | None, scheduled: list[dict] | None, now: datetime) -> float | None:
    """Delivered over scheduled service hours on recent days that have both. Days the trains
    outran the schedule can push this a little past 100%."""
    if not extended or not scheduled:
        return None
    delivered: dict[str, float] = {}
    for row in extended:
        delivered[row["date"]] = delivered.get(row["date"], 0) + _delivered_hours(row)
    planned = {row["date"]: row["serviceMinutes"] / 60 for row in scheduled if row.get("serviceMinutes")}
    days = _recent_days({day for day in delivered if delivered[day] and day in planned}, now)
    total = sum(planned[day] for day in days)
    return sum(delivered[day] for day in days) / total if total else None


def bus_delivered_share(rows: list[dict] | None, now: datetime) -> float | None:
    """Scheduled trips LAMP saw a bus on, over all scheduled trips, on recent days."""
    with_trips = [row for row in rows or [] if row.get("scheduled_trips")]
    days = _recent_days({row["date"] for row in with_trips}, now)
    recent = [row for row in with_trips if row["date"] in days]
    scheduled = sum(row["scheduled_trips"] for row in recent)
    if not scheduled:
        return None
    return 1 - sum(row.get("unobserved_trips") or 0 for row in recent) / scheduled


def with_delivered(cell: dict, share: float | None, what: str) -> dict:
    """The Service cell with the delivered share after its trips per day."""
    if share is None:
        return cell
    # Shown capped: a day or two over schedule reads as noise, not extra service.
    text = f"{js_round(min(share, 1) * 100)}% of scheduled {what} delivered"
    return {**cell, "secondary": f"{cell['secondary']} · {text}" if cell.get("secondary") else text}
