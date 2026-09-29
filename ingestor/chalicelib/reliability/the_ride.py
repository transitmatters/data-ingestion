import csv
import io
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

import requests

from ..dynamo import dynamo_batch_write
from ..ridership.config import THE_RIDE_RIDERSHIP_ARCGIS_URL, THE_RIDE_UPDATE_CACHE_URL
from .constants import DYNAMO_TABLE_NAME

ROUTE_ID = "RIDE"

# Deprecated dataset covering 2014-07-01 through 2025-07-31; only used for backfills.
THE_RIDE_LEGACY_RELIABILITY_ARCGIS_URL = "https://opendata.arcgis.com/api/v3/datasets/cb4f4fb3cdf443e7a9d66c87ab1f5e17_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"

# No-shows and missed trips weren't recorded before this date; the dataset reports them as 0.
NO_SHOWS_RECORDED_FROM = date(2026, 1, 7)

REQUEST_TIMEOUT = 60


@dataclass
class TheRideReliabilityEntry:
    date: date
    completed: int
    on_time: int
    requests: Optional[int] = None
    no_shows: Optional[int] = None
    missed: Optional[int] = None
    otp: Optional[Decimal] = None
    source: str = "current"

    def to_item(self) -> Dict:
        item = {
            "routeId": ROUTE_ID,
            "mode": "the-ride",
            "date": self.date.isoformat(),
            "timestamp": int(datetime.combine(self.date, datetime.min.time()).timestamp()),
            "completed": self.completed,
            "onTime": self.on_time,
            "source": self.source,
        }
        optional = {
            "requests": self.requests,
            "noShows": self.no_shows,
            "missed": self.missed,
            "otp": self.otp,
        }
        item.update({key: value for key, value in optional.items() if value is not None})
        return item


def parse_trip_date(value: str) -> date:
    """Parse ArcGIS dates like '2025/10/24 04:00:00+00' (midnight Eastern, in UTC) to a service date."""
    return datetime.strptime(value[:10].replace("/", "-"), "%Y-%m-%d").date()


def parse_current_row(row: Dict[str, str]) -> TheRideReliabilityEntry:
    trip_date = parse_trip_date(row["TripDate"])
    recorded = trip_date >= NO_SHOWS_RECORDED_FROM
    return TheRideReliabilityEntry(
        date=trip_date,
        requests=int(row["TotalRequests"]),
        completed=int(row["CompletedTrips"]),
        on_time=int(row["CompletedOnTime"]),
        no_shows=int(row["TotalNoShows"]) if recorded else None,
        missed=int(row["TotalMissedTrips"]) if recorded else None,
        # MBTA's OTP counts no-shows as on time: (on time + no-shows) / (completed + missed + no-shows)
        otp=Decimal(row["OTP"]) if row["OTP"] else None,
    )


def parse_legacy_row(row: Dict[str, str]) -> TheRideReliabilityEntry:
    return TheRideReliabilityEntry(
        date=parse_trip_date(row["trip_date"]),
        completed=int(row["trip_count"]),
        on_time=int(row["ontime_trip_count"]),
        source="legacy",
    )


def read_csv_rows(text: str) -> Iterable[Dict[str, str]]:
    # The ArcGIS CSV exports start with a UTF-8 BOM
    return csv.DictReader(io.StringIO(text.lstrip("﻿")))


def fetch_csv(url: str) -> str:
    req = requests.get(url, timeout=REQUEST_TIMEOUT)
    req.raise_for_status()
    return req.content.decode("utf-8-sig")


def merge_entries(
    current: Iterable[TheRideReliabilityEntry], legacy: Iterable[TheRideReliabilityEntry]
) -> List[TheRideReliabilityEntry]:
    """Combine datasets, preferring the current dataset on any overlapping dates."""
    by_date = {entry.date: entry for entry in legacy}
    by_date.update({entry.date: entry for entry in current})
    return [by_date[d] for d in sorted(by_date)]


def filter_lookback(entries: Iterable[TheRideReliabilityEntry], lookback_days: Optional[int], today: date = None):
    if lookback_days is None:
        return list(entries)
    cutoff = (today or date.today()) - timedelta(days=lookback_days)
    return [entry for entry in entries if entry.date >= cutoff]


def load_entries(include_legacy: bool) -> List[TheRideReliabilityEntry]:
    # The Hub doesn't refresh this dataset's download cache on its own
    requests.get(THE_RIDE_UPDATE_CACHE_URL, timeout=REQUEST_TIMEOUT)
    current = [parse_current_row(row) for row in read_csv_rows(fetch_csv(THE_RIDE_RIDERSHIP_ARCGIS_URL))]
    legacy = []
    if include_legacy:
        legacy = [parse_legacy_row(row) for row in read_csv_rows(fetch_csv(THE_RIDE_LEGACY_RELIABILITY_ARCGIS_URL))]
    return merge_entries(current, legacy)


def update_the_ride_reliability(lookback_days: Optional[int] = 120, include_legacy: bool = False, dry_run=False):
    """Ingest daily The RIDE reliability (requests, on-time, no-shows, missed trips) into DynamoDB."""
    entries = filter_lookback(load_entries(include_legacy), lookback_days)
    items = [entry.to_item() for entry in entries]
    print(
        f"The RIDE reliability: {len(items)} days ({items[0]['date'] if items else '-'} to {items[-1]['date'] if items else '-'})"
    )
    if not dry_run:
        dynamo_batch_write(items, DYNAMO_TABLE_NAME)
    return items


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill The RIDE reliability, including the deprecated dataset")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    update_the_ride_reliability(lookback_days=None, include_legacy=True, dry_run=args.dry_run)
