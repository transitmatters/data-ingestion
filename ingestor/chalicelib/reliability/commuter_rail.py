import csv
import io
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from ..dynamo import dynamo_batch_write

DYNAMO_TABLE_NAME = "CommuterRailReliability"

CR_RELIABILITY_ARCGIS_URL = "https://www.arcgis.com/sharing/rest/content/items/ec18161c237d419698abc767f1be6a50/data"

SCHEDULE_ADHERENCE_METRIC = "Headway / Schedule Adherence"

PERIOD_KEYS = {"PEAK": "peak", "OFF_PEAK": "offPeak"}

REQUEST_TIMEOUT = 120

EntryKey = Tuple[str, date]


def empty_counts() -> Dict[str, int]:
    return {"otpNumerator": 0, "otpDenominator": 0, "cancelled": 0}


def parse_service_date(value: str) -> date:
    return datetime.strptime(value, "%m/%d/%Y").date()


def aggregate_rows(
    rows: Iterable[Dict[str, str]], lookback_days: Optional[int] = None, today: date = None
) -> Dict[EntryKey, Dict]:
    """Sum on-time and cancellation counts per route and service date, split by peak/off-peak.

    Branches that share a GTFS route (e.g. Fall River and New Bedford on CR-NewBedford, or
    Franklin via Fairmount on CR-Franklin) are reported as separate rows and summed here.
    """
    cutoff = (today or date.today()) - timedelta(days=lookback_days) if lookback_days is not None else None
    by_key: Dict[EntryKey, Dict] = {}
    for row in rows:
        if row["metric_type"] != SCHEDULE_ADHERENCE_METRIC:
            continue
        service_date = parse_service_date(row["service_date"])
        if cutoff and service_date < cutoff:
            continue
        period = PERIOD_KEYS.get(row["peak_offpeak_ind"])
        if period is None:
            print(f"Unknown peak_offpeak_ind {row['peak_offpeak_ind']!r} for {row['gtfs_route_id']} on {service_date}")
            continue
        key = (row["gtfs_route_id"], service_date)
        entry = by_key.setdefault(key, {"peak": empty_counts(), "offPeak": empty_counts()})
        counts = entry[period]
        counts["otpNumerator"] += int(row["otp_numerator"])
        counts["otpDenominator"] += int(row["otp_denominator"])
        counts["cancelled"] += int(row["cancelled_numerator"])
    return by_key


def to_items(by_key: Dict[EntryKey, Dict]) -> List[Dict]:
    items = []
    for (route_id, service_date), periods in sorted(by_key.items()):
        totals = {field: periods["peak"][field] + periods["offPeak"][field] for field in empty_counts()}
        items.append(
            {
                "routeId": route_id,
                "date": service_date.isoformat(),
                "timestamp": int(datetime.combine(service_date, datetime.min.time()).timestamp()),
                **totals,
                **periods,
            }
        )
    return items


def load_rows() -> Iterable[Dict[str, str]]:
    req = requests.get(CR_RELIABILITY_ARCGIS_URL, timeout=REQUEST_TIMEOUT)
    req.raise_for_status()
    return csv.DictReader(io.StringIO(req.content.decode("utf-8-sig")))


def update_commuter_rail_reliability(lookback_days: Optional[int] = 180, dry_run=False):
    """Ingest daily commuter rail on-time performance and cancellations by route into DynamoDB."""
    items = to_items(aggregate_rows(load_rows(), lookback_days))
    routes = sorted({item["routeId"] for item in items})
    print(f"Commuter rail reliability: {len(items)} route-days across {len(routes)} routes: {routes}")
    if not dry_run:
        dynamo_batch_write(items, DYNAMO_TABLE_NAME)
    return items


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill the full commuter rail reliability history")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    update_commuter_rail_reliability(lookback_days=None, dry_run=args.dry_run)
