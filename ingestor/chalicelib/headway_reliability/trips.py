"""Dropped subway trips: trips the schedule sends through a stop that no train made.

At each stop, the trains seen stopping there (distinct trips with an arrival or departure, including
ones the MBTA ran off schedule as ADDED- trips) are set against the trips scheduled through it, and
the stop's shortfall is max(0, scheduled - trains). Each line direction takes the median stop's
share dropped, so one stop missing a day's data can't move it, out of the trips scheduled through a
typical stop. Green Line branches count only at stops their branch serves alone, since a trunk stop
can't say which branch a train was.
"""

from statistics import median

import pandas as pd

LINE_ROUTES = {
    "line-red": ["Red"],
    "line-orange": ["Orange"],
    "line-blue": ["Blue"],
    "line-mattapan": ["Mattapan"],
    "line-green": ["Green-B", "Green-C", "Green-D", "Green-E"],
}
BRANCHED_LINES = {"line-green"}

# A direction needs this share of its stops to have both a schedule and a day's events.
MIN_STOP_SHARE = 0.5

TRIP_FIELDS = ["scheduled_trips", "dropped_trips"]

# (stop, route) -> trips scheduled through the stop that day
Scheduled = dict[tuple[str, str], int]


def trains_seen(events: pd.DataFrame) -> dict[str, int]:
    """Distinct trips that arrived at or departed one stop, by service date."""
    stops = events[events["event_type"].isin(["ARR", "DEP"])]
    return {day: int(n) for day, n in stops.groupby("service_date")["trip_id"].nunique().items()}


def _groups(line: str, stops: list[str], scheduled: Scheduled) -> list[dict[str, int]]:
    """{stop: trips scheduled} for each group of stops that sees the same trains."""
    routes = LINE_ROUTES[line]
    by_stop: dict[str, dict[str, int]] = {}
    for stop in stops:
        for route in routes:
            if scheduled.get((stop, route)):
                by_stop.setdefault(stop, {})[route] = scheduled[(stop, route)]
    if line not in BRANCHED_LINES:
        return [{stop: sum(counts.values()) for stop, counts in by_stop.items()}]
    return [{stop: counts[route] for stop, counts in by_stop.items() if set(counts) == {route}} for route in routes]


def line_trip_counts(
    line: str, stops_by_direction: dict[str, list[str]], scheduled: Scheduled, trains: dict[str, int | None]
) -> dict[str, int] | None:
    """{scheduled_trips, dropped_trips} for one line on one day. `trains` is None for a stop with no
    events that day. None if any direction or branch lacks enough stops to judge."""
    scheduled_total = dropped_total = 0.0
    for stops in stops_by_direction.values():
        for group in _groups(line, stops, scheduled):
            if not group:
                continue
            seen = [(n, trains[stop]) for stop, n in group.items() if trains.get(stop) is not None]
            if len(seen) < max(1, MIN_STOP_SHARE * len(group)):
                return None
            typical = median(n for n, _ in seen)
            scheduled_total += typical
            dropped_total += typical * median(max(0, n - ran) / n for n, ran in seen)
    if not scheduled_total:
        return None
    return {"scheduled_trips": round(scheduled_total), "dropped_trips": round(dropped_total)}
