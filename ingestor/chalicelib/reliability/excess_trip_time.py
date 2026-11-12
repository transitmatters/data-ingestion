"""Excess trip time (ETT) on the Red, Orange and Blue Lines, from the MBTA open data portal.

ETT is the MBTA's subway reliability measure in its Service Delivery Policy: how much longer a
rider's trip (waiting on the platform plus riding) took than a baseline trip. The baseline is
not the schedule. It's the fastest monthly median travel time since 2021 plus the wait the best
possible schedule would give. Riders, their origins and destinations come from fare taps (the
MBTA's ODX model), and the dataset reports them per day, half hour, line and direction.

Every column is a passenger count or passenger-weighted minutes, so items stay sums: the
dashboard adds them up over any period and divides by ``riders`` itself. Each item has the
line's daily totals with the same sums per service period under ``byPeriod``, and both again per
direction under ``byDirection``, so the dashboard's direction filter applies to every chart.

The source CSV is ~95 MB and published monthly, about five weeks behind. It's streamed
rather than loaded whole, so the Lambda stays at 512 MB.
"""

import codecs
import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from ..dynamo import dynamo_batch_write
from .constants import DYNAMO_TABLE_NAME

ETT_ARCGIS_URL = "https://www.arcgis.com/sharing/rest/content/items/031c3392b4fd46baad54fcceb9fb4254/data"

# The dataset's route_id. It's expected to add the Green Line later, in a format not yet known.
ROUTE_IDS = {"Red": "line-red", "Orange": "line-orange", "Blue": "line-blue"}

# GTFS direction_id (0 is south/westbound), the same keys as the HeadwayReliability table.
DIRECTION_IDS = {"SB": "0", "NB": "1", "WB": "0", "EB": "1"}

PERIOD_KEYS = {"AM PEAK": "amPeak", "PM PEAK": "pmPeak", "OFF-PEAK": "offPeak"}

# CSV column -> item field. Actual trip time is always wait plus travel, so it isn't stored.
# ETT is (wait + travel) - (baseline wait + baseline travel). Baseline trip time is kept as
# published even so: in ~3% of rows it's a little under baseline wait plus travel (about a
# second per rider over a month).
FIELDS = {
    "est_pax": "riders",
    "weighted_wait_time": "waitTime",
    "weighted_travel_time": "travelTime",
    "weighted_bl_trip_time": "baselineTripTime",
    "weighted_bl_wait_time": "baselineWaitTime",
    "weighted_bl_travel_time": "baselineTravelTime",
    "five_min_thresh": "withinFiveMin",
    "twenty_min_thresh": "withinTwentyMin",
    "twenty_pct_thresh": "withinTwentyPct",
    "thirty_pct_thresh": "withinThirtyPct",
}

REQUEST_TIMEOUT = 120

EntryKey = Tuple[str, date]


def empty_counts() -> Dict[str, float]:
    return {field: 0.0 for field in FIELDS.values()}


def empty_scope() -> Dict:
    return {"totals": empty_counts(), "byPeriod": {period: empty_counts() for period in PERIOD_KEYS.values()}}


def empty_entry() -> Dict:
    """Every direction and period starts at zero, so all items share one shape and the dashboard
    never drops a direction or period when it sums a span with a gap in it."""
    return {
        **empty_scope(),
        "byDirection": {direction: empty_scope() for direction in sorted(set(DIRECTION_IDS.values()))},
    }


def aggregate_rows(
    rows: Iterable[Dict[str, str]], lookback_days: Optional[int] = None, today: Optional[date] = None
) -> Dict[EntryKey, Dict]:
    """Sum the half-hourly rows into one entry per line and service date."""
    cutoff = (
        ((today or date.today()) - timedelta(days=lookback_days)).isoformat() if lookback_days is not None else None
    )
    by_key: Dict[EntryKey, Dict] = {}
    skipped = set()
    for row in rows:
        # svc_date is YYYY-MM-DD, so old rows can be skipped before parsing any numbers
        if cutoff and row["svc_date"] < cutoff:
            continue
        route_id = ROUTE_IDS.get(row["route_id"])
        direction = DIRECTION_IDS.get(row["dir_id"])
        period = PERIOD_KEYS.get(row["peak"])
        if route_id is None or direction is None or period is None:
            skipped.add((row["route_id"], row["dir_id"], row["peak"]))
            continue
        entry = by_key.setdefault((route_id, date.fromisoformat(row["svc_date"])), empty_entry())
        groups = [
            counts
            for scope in (entry, entry["byDirection"][direction])
            for counts in (scope["totals"], scope["byPeriod"][period])
        ]
        for column, field in FIELDS.items():
            value = float(row[column])
            for counts in groups:
                counts[field] += value
    for route, direction, period in sorted(skipped):
        print(f"Excess trip time: skipped unknown route/direction/period {route!r} {direction!r} {period!r}")
    return by_key


def to_decimals(counts: Dict[str, float]) -> Dict[str, Decimal]:
    """DynamoDB needs Decimals. A tenth of a rider or minute is plenty: a line carries tens of
    thousands of riders a day."""
    return {field: Decimal(f"{value:.1f}") for field, value in counts.items()}


def scope_fields(scope: Dict) -> Dict:
    """A scope's totals as top-level fields, with its per-period sums under ``byPeriod``."""
    return {
        **to_decimals(scope["totals"]),
        "byPeriod": {period: to_decimals(counts) for period, counts in scope["byPeriod"].items()},
    }


def to_items(by_key: Dict[EntryKey, Dict]) -> List[Dict]:
    items = []
    for (route_id, service_date), entry in sorted(by_key.items()):
        items.append(
            {
                "routeId": route_id,
                "mode": "subway",
                "date": service_date.isoformat(),
                "timestamp": int(datetime.combine(service_date, datetime.min.time()).timestamp()),
                **scope_fields(entry),
                "byDirection": {direction: scope_fields(scope) for direction, scope in entry["byDirection"].items()},
            }
        )
    return items


def load_rows() -> Iterable[Dict[str, str]]:
    """Stream the CSV row by row instead of holding all ~95 MB of it in memory."""
    req = requests.get(ETT_ARCGIS_URL, timeout=REQUEST_TIMEOUT, stream=True)
    req.raise_for_status()
    lines = codecs.iterdecode(req.iter_lines(chunk_size=1 << 20), "utf-8-sig")
    return csv.DictReader(lines)


def update_excess_trip_time(lookback_days: Optional[int] = 120, dry_run=False):
    """Ingest daily excess trip time by subway line into DynamoDB.

    The MBTA may restate recent weeks, so the last ``lookback_days`` are rewritten each run.
    """
    items = to_items(aggregate_rows(load_rows(), lookback_days))
    routes = sorted({item["routeId"] for item in items})
    dates = [item["date"] for item in items]
    print(
        f"Excess trip time: {len(items)} line-days across {routes}"
        + (f", {min(dates)} to {max(dates)}" if dates else "")
    )
    if not dry_run:
        dynamo_batch_write(items, DYNAMO_TABLE_NAME)
    return items


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill the full excess trip time history (from 2022)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    update_excess_trip_time(lookback_days=None, dry_run=args.dry_run)
