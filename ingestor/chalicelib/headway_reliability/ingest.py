"""Daily headway reliability and dropped trips per subway line, written to the HeadwayReliability table.

Every non-terminal stop counts, weighted by how many headways it has. Items hold raw bucket counts
so the dashboard and the home page can roll them up over any period and pick their own band. Bus
counts are written per route by mbta-performance into DeliveredTripMetricsBus, with the same names.
Dropped trips (trips.py) come from the same stops' events, against the GTFS schedule.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from itertools import groupby
from pathlib import Path

import boto3
import pandas as pd

from . import events, schedule
from .compute import HEADWAY_FIELDS, KEYS, headway_counts
from .trips import line_trip_counts, trains_seen

TABLE_NAME = "HeadwayReliability"

# Non-terminal platform stop ids per line and direction, from the dashboard's stations.json.
SUBWAY_STOPS: dict[str, dict[str, list[str]]] = json.loads((Path(__file__).parent / "subway_stops.json").read_text())

MAX_WORKERS = 32


def _stop_counts(line: str, stop: str, days: list[date], archived: bool) -> list[tuple[str, str, pd.DataFrame, dict]]:
    """(line, source, counts, trains seen by date) for one stop, from the monthly archive or each
    day's best source."""
    if archived:
        df = events.read_events(events.key_for(events.MONTHLY, stop, days[0]))
        if df is None:
            return []
        df = df[df["service_date"].isin({d.isoformat() for d in days})]
        return [(line, "monthly", headway_counts(df), trains_seen(df))]

    found = []
    for day in days:
        for source, template in events.DAILY:
            df = events.read_events(events.key_for(template, stop, day))
            if df is not None and not df.empty:
                found.append((line, source, headway_counts(df), trains_seen(df)))
                break
    return found


def to_items(results: list[tuple[str, str, pd.DataFrame]]) -> list[dict]:
    frames = [counts.assign(route=line, source=source) for line, source, counts in results if not counts.empty]
    if not frames:
        return []
    df = pd.concat(frames)
    by_direction = df.groupby(["route", *KEYS])[HEADWAY_FIELDS].sum()
    sources = df.groupby(["route", "service_date"])["source"].agg(lambda s: "+".join(sorted(set(s))))

    items = []
    for (route, day), directions in by_direction.groupby(level=["route", "service_date"]):
        totals = directions.sum()
        items.append(
            {
                "route": route,
                "date": day,
                "source": sources[(route, day)],
                **{field: int(totals[field]) for field in HEADWAY_FIELDS},
                "byDirection": {
                    direction: {field: int(row[field]) for field in HEADWAY_FIELDS}
                    for (_, _, direction), row in directions.iterrows()
                },
            }
        )
    return items


def with_trip_counts(items: list[dict], trains: dict[tuple[str, str], int], scheduled: dict[date, dict]) -> list[dict]:
    """Adds scheduled_trips and dropped_trips to each line's day, where the schedule and enough
    stops' events allow. `trains` is keyed by (stop, date)."""
    for item in items:
        day = item["date"]
        on_day = scheduled.get(date.fromisoformat(day))
        if on_day is None:
            continue
        stops = SUBWAY_STOPS[item["route"]]
        seen = {stop: trains.get((stop, day)) for ids in stops.values() for stop in ids}
        counts = line_trip_counts(item["route"], stops, on_day, seen)
        if counts:
            item.update(counts)
    return items


def _stops() -> list[tuple[str, str]]:
    return [(line, stop) for line, dirs in SUBWAY_STOPS.items() for ids in dirs.values() for stop in ids]


def compute_days(days: list[date], scheduled: dict[date, dict] | None = None) -> list[dict]:
    """Items for days within one calendar month. Days in `scheduled` (schedule.scheduled_trips) also
    get trip counts."""
    assert len({(d.year, d.month) for d in days}) == 1, "days must fall in one month"
    archived = events.month_archived(days[0])
    stops = _stops()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        results = list(pool.map(lambda s: _stop_counts(*s, days, archived), stops))
    trains = {
        (stop, day): n
        for (_, stop), stop_results in zip(stops, results)
        for *_, seen in stop_results
        for day, n in seen.items()
    }
    items = to_items([(line, source, counts) for stop_results in results for line, source, counts, _ in stop_results])
    return with_trip_counts(items, trains, scheduled or {})


def write_items(items: list[dict]):
    table = boto3.resource("dynamodb").Table(TABLE_NAME)
    with table.batch_writer(overwrite_by_pkeys=["route", "date"]) as batch:
        for item in items:
            batch.put_item(Item=item)


def update_days(start: date, end: date):
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    failures = []
    for _, month in groupby(days, key=lambda d: (d.year, d.month)):
        month = list(month)
        try:
            scheduled = schedule.scheduled_trips(month, {stop for _, stop in _stops()})
        except Exception as e:  # headway reliability doesn't need the schedule, so write it and raise after
            print(f"Couldn't read the GTFS schedule for {month[0]} to {month[-1]}: {e!r}")
            failures.append(e)
            scheduled = {}
        items = compute_days(month, scheduled)
        print(f"Writing {len(items)} headway reliability items for {month[0]} to {month[-1]}")
        write_items(items)
    if failures:
        raise RuntimeError(f"Wrote headway reliability without trip counts for {len(failures)} month(s)") from failures[
            0
        ]
