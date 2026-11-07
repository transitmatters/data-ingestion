"""Headway reliability: how close each headway lands to the scheduled one.

A headway is the time between consecutive departures at a stop, from any branch serving it, so
trunk stops pool their branches the way riders wait for them. Each headway is bucketed by its ratio
to the scheduled headway the ingest wrote on the same event row. These are the rules the
dashboard's /api/headways uses (server/chalicelib/s3_historical.py), so line totals agree with its
per-stop charts.
"""

import pandas as pd

from .buckets import HEADWAY_FIELDS, HEADWAY_RATIO_BUCKETS

MAX_HEADWAY_SEC = 120 * 60

COLUMNS = ["service_date", "trip_id", "direction_id", "event_type", "event_time", "scheduled_headway"]
KEYS = ["service_date", "direction_id"]


def empty_counts() -> pd.DataFrame:
    return pd.DataFrame(columns=[*KEYS, *HEADWAY_FIELDS])


def _in_bucket(ratio: pd.Series, lower: float, upper: float, includes_lower: bool, includes_upper: bool) -> pd.Series:
    above = ratio >= lower if includes_lower else ratio > lower
    below = ratio <= upper if includes_upper else ratio < upper
    return above & below


def _event_times(times: pd.Series) -> pd.Series:
    """LAMP times carry a UTC offset, which keeps the fall-back hour in order; the monthly archives
    are bare Eastern wall-clock times."""
    if times.str.len().gt(19).any():
        return pd.to_datetime(times, utc=True, format="ISO8601")
    return pd.to_datetime(times, format="ISO8601")


def headway_counts(events: pd.DataFrame) -> pd.DataFrame:
    """Headway counts per bucket, one row per (service_date, direction_id), for one stop's events."""
    deps = events[events["event_type"] == "DEP"]
    if deps.empty:
        return empty_counts()

    deps = deps.assign(
        direction_id=deps["direction_id"].astype(str).str.replace(r"\.0$", "", regex=True),
        trip_id=deps["trip_id"].fillna("").astype(str),
        event_time=_event_times(deps["event_time"].astype(str)),
        scheduled_headway=pd.to_numeric(deps["scheduled_headway"], errors="coerce"),
    ).sort_values("event_time")

    group = deps.groupby(KEYS, sort=False)
    headway = group["event_time"].diff().dt.total_seconds()
    # The same trip departing twice (rare) isn't a headway.
    same_trip = (deps["trip_id"] != "") & (deps["trip_id"] == group["trip_id"].shift())
    scheduled = deps["scheduled_headway"]
    usable = headway.notna() & ~same_trip & (headway <= MAX_HEADWAY_SEC) & (scheduled > 0)
    if not usable.any():
        return empty_counts()

    ratio = headway[usable] / scheduled[usable]
    buckets = {name: _in_bucket(ratio, *edges) for name, *edges in HEADWAY_RATIO_BUCKETS}
    return deps.loc[usable, KEYS].assign(**buckets).groupby(KEYS).sum().astype(int).reset_index()
