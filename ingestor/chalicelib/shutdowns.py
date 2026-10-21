"""Shutdowns (shuttles, suspensions, station closures) from the LAMP alert history.

Charts go blank during a shutdown. This indexes the alerts that explain those gaps
into the Shutdowns table, one item per (line, alert), so the dashboard can label a
date range with one query instead of reading every day's LAMP alerts file.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from typing import Optional

from . import constants, dynamo
from .date_utils import EASTERN_TIME, service_date
from .delays.duration import load_lamp_alerts

TABLE_NAME = "Shutdowns"

SHUTDOWN_EFFECTS = {"SHUTTLE", "SUSPENSION", "STATION_CLOSURE"}

# Shuttles for an incident ("disabled train at Harvard") are usually gone within an
# hour or two. Planned diversions run at least an evening.
MIN_PERIOD = timedelta(hours=3)

# mbta-performance rebuilds this many days of LAMP files each morning, ending yesterday.
LOOKBACK_DAYS = 30


def _parse(ts: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(ts).astimezone(EASTERN_TIME) if ts else None


def _service_day_end(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 3, tzinfo=EASTERN_TIME) + timedelta(days=1)


def _end_service_date(end: datetime) -> date:
    # Periods end exclusively, so one ending at the 3am rollover belongs to the day before.
    return service_date(end - timedelta(seconds=1))


def shutdown_periods(attrs: dict, last_day: date) -> list[tuple[datetime, datetime]]:
    """(start, end) of each active period that ran at least MIN_PERIOD.

    Periods are cut short at closed_timestamp, when MBTA took the alert down. A
    period with no end and no close is still ongoing, so it runs through `last_day`,
    the latest day file the alert was seen in.
    """
    closed = _parse(attrs.get("closed_timestamp"))
    periods = []
    for period in attrs.get("active_period", []):
        start = _parse(period.get("start"))
        if start is None or (closed and start >= closed):
            continue
        end = _parse(period.get("end"))
        if closed:
            end = min(end, closed) if end else closed
        if end is None:
            end = _service_day_end(last_day)
        if end - start >= MIN_PERIOD:
            periods.append((start, end))
    return sorted(periods)


def shutdown_items(alert: dict, last_day: date) -> list[dict]:
    """Shutdowns table items for one LAMP alert: one per line it informs, or none if it isn't a shutdown."""
    attrs = alert["attributes"]
    if attrs.get("effect") not in SHUTDOWN_EFFECTS:
        return []
    periods = shutdown_periods(attrs, last_day)
    if not periods:
        return []

    stops_by_line: dict[str, set[str]] = {}
    for entity in attrs.get("informed_entity", []):
        line = entity.get("route")
        if line not in constants.ALL_LINES:
            continue
        stops = stops_by_line.setdefault(line, set())
        if entity.get("stop"):
            stops.add(entity["stop"])

    return [
        {
            "line": line,
            "alert_id": alert["id"],
            "effect": attrs["effect"],
            "cause": attrs.get("cause"),
            "header": attrs.get("header") or attrs.get("service_effect") or "",
            "stops": sorted(stops),
            "periods": [{"start": start.isoformat(), "end": end.isoformat()} for start, end in periods],
            "first_start": service_date(periods[0][0]).isoformat(),
            "last_end": max(_end_service_date(end) for _, end in periods).isoformat(),
        }
        for line, stops in stops_by_line.items()
    ]


def collect_shutdowns(start_date: date, end_date: date) -> list[dict]:
    """Shutdown items from every LAMP day file in [start_date, end_date].

    An alert appears in every day file it's active on. The latest day's copy wins,
    since it carries the alert's latest revision.
    """
    days = [start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)]
    items: dict[tuple[str, str], dict] = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        for day, alerts in zip(days, executor.map(load_lamp_alerts, days)):
            for alert in (alerts or {}).values():
                for item in shutdown_items(alert, day):
                    items[(item["line"], item["alert_id"])] = item
    return list(items.values())


def update_table(start_date: date, end_date: date):
    """Writes shutdowns from LAMP day files in [start_date, end_date] to the Shutdowns table.

    For a backfill, run ranges in date order: an alert that spans two ranges is
    rewritten by the later one, with its later revision.
    """
    items = collect_shutdowns(start_date, end_date)
    print(f"Writing {len(items)} shutdown items from {start_date} to {end_date}")
    dynamo.dynamo_batch_write(items, TABLE_NAME)


def update_recent():
    """Re-reads the LAMP day files mbta-performance just rebuilt, picking up new and revised shutdowns."""
    yesterday = date.today() - timedelta(days=1)
    update_table(yesterday - timedelta(days=LOOKBACK_DAYS), yesterday)
