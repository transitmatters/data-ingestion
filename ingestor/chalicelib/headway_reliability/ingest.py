"""Daily headway reliability per subway line, written to the HeadwayReliability table.

Every non-terminal stop counts, weighted by how many headways it has. Items hold raw bucket counts
so the dashboard and the home page can roll them up over any period and pick their own band. Bus
counts are written per route by mbta-performance into DeliveredTripMetricsBus, with the same names.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from itertools import groupby
from pathlib import Path

import boto3
import pandas as pd

from . import events
from .compute import HEADWAY_FIELDS, KEYS, headway_counts

TABLE_NAME = "HeadwayReliability"

# Non-terminal platform stop ids per line and direction, from the dashboard's stations.json.
SUBWAY_STOPS: dict[str, dict[str, list[str]]] = json.loads((Path(__file__).parent / "subway_stops.json").read_text())

MAX_WORKERS = 32


def _stop_counts(line: str, stop: str, days: list[date], archived: bool) -> list[tuple[str, str, pd.DataFrame]]:
    """(line, source, counts) for one stop, from the monthly archive or each day's best source."""
    if archived:
        df = events.read_events(events.key_for(events.MONTHLY, stop, days[0]))
        if df is None:
            return []
        return [(line, "monthly", headway_counts(df[df["service_date"].isin({d.isoformat() for d in days})]))]

    found = []
    for day in days:
        for source, template in events.DAILY:
            df = events.read_events(events.key_for(template, stop, day))
            if df is not None and not df.empty:
                found.append((line, source, headway_counts(df)))
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


def compute_days(days: list[date]) -> list[dict]:
    """Items for days within one calendar month."""
    assert len({(d.year, d.month) for d in days}) == 1, "days must fall in one month"
    archived = events.month_archived(days[0])
    stops = [(line, stop) for line, dirs in SUBWAY_STOPS.items() for ids in dirs.values() for stop in ids]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        results = pool.map(lambda s: _stop_counts(*s, days, archived), stops)
        return to_items([r for stop_results in results for r in stop_results])


def write_items(items: list[dict]):
    table = boto3.resource("dynamodb").Table(TABLE_NAME)
    with table.batch_writer(overwrite_by_pkeys=["route", "date"]) as batch:
        for item in items:
            batch.put_item(Item=item)


def update_days(start: date, end: date):
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    for _, month in groupby(days, key=lambda d: (d.year, d.month)):
        month = list(month)
        items = compute_days(month)
        print(f"Writing {len(items)} headway reliability items for {month[0]} to {month[-1]}")
        write_items(items)
