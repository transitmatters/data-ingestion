import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import boto3

from . import constants
from .car_ages import average_age, mix_percentages
from .fleet_events import merge_rows, vehicle_ids_at_stop

TABLE_NAME = "DeliveredTripMetricsCR"
SYSTEM_WIDE_KEY = "all"

# Commuter Rail is only captured by gobble: no LAMP or monthly archives.
EVENT_KEY_TEMPLATES = ["Events-live/daily-cr-data/{stop}/Year={year}/Month={month}/Day={day}/events.csv.gz"]

delivered_trip_metrics = boto3.resource("dynamodb").Table(TABLE_NAME)

# The MBTA feed identifies each train by its cab car (control coach), not its locomotive or
# coaches, so these are cab cars plus the locomotives that occasionally show up instead.
# Number range -> (build year, type), from the NETransit MBTA roster (09/23/26).
CR_FLEET: dict[str, tuple[float | None, str]] = {
    "1500-1533": (1987.5, "mbb"),  # CTC-3
    "1600-1652": (1989.5, "bombardier"),  # CTC-1B
    "1700-1724": (1990.5, "kawasaki"),  # CTC-4
    "1800-1827": (2013, "rotem"),  # CTC-5
    "1828-1870": (2023, "rotem"),  # CTC-5
    "1400-1409": (None, "rotem"),  # on order: add build year on delivery
    "1001-1006": (None, "locomotive"),  # leased F40PH-4C
    "1025-1036": (1992, "locomotive"),  # F40PH-3C
    "1050-1075": (1987.5, "locomotive"),  # F40PH-3C
    "1115-1139": (1974, "locomotive"),  # GP40MC
    "1776-1776": (1974, "locomotive"),  # GP40MC, ex 1131
    "2000-2039": (2013.5, "locomotive"),  # HSP-46
}
CR_TYPES = ["mbb", "bombardier", "kawasaki", "rotem", "locomotive"]

# Stops sampled per route: the busiest stop gobble captures in each direction.
CR_FLEET_STOPS: dict[str, list[str]] = json.loads((Path(__file__).parent / "cr_fleet_stops.json").read_text())


def get_cr_info(vehicle_id: int) -> tuple[float | None, str] | None:
    """(build year, type) for a Commuter Rail vehicle number, or None if unknown."""
    for range_str, info in CR_FLEET.items():
        low, high = range_str.split("-")
        if int(low) <= vehicle_id <= int(high):
            return info
    return None


def compute_cr_metrics(trip_vehicle_ids: list[int], current_date: date) -> dict[str, Decimal]:
    """Fleet metrics for one vehicle reported per trip. Unknown numbers are left out."""
    counts = {cr_type: 0 for cr_type in CR_TYPES}
    build_years: dict[int, float] = {}
    for vehicle_id in trip_vehicle_ids:
        info = get_cr_info(vehicle_id)
        if info is None:
            continue
        year, cr_type = info
        counts[cr_type] += 1
        if year is not None and cr_type != "locomotive":
            build_years[vehicle_id] = year

    trips = sum(counts.values())
    if not trips:
        return {}
    metrics: dict[str, Decimal] = {"fleet_trips": Decimal(trips), **mix_percentages(counts)}
    if build_years:
        metrics["avg_cab_car_age"] = average_age(list(build_years.values()), current_date)
    return metrics


def _vehicle_ids_for_route(route: str, current_date: date) -> list[int]:
    return [
        vehicle_id
        for stop_id in CR_FLEET_STOPS[route]
        for vehicle_id in vehicle_ids_at_stop(stop_id, current_date, EVENT_KEY_TEMPLATES)
    ]


def get_cr_fleet_metrics(current_date: date) -> list[dict]:
    """One row per sampled route, plus a system-wide row pooled across every route's trips."""
    with ThreadPoolExecutor(max_workers=8) as executor:
        route_ids = dict(
            zip(CR_FLEET_STOPS, executor.map(lambda r: _vehicle_ids_for_route(r, current_date), CR_FLEET_STOPS))
        )
    date_str = current_date.strftime(constants.DATE_FORMAT_BACKEND)
    rows = []
    for route, vehicle_ids in [*route_ids.items(), (SYSTEM_WIDE_KEY, sum(route_ids.values(), []))]:
        metrics = compute_cr_metrics(vehicle_ids, current_date)
        if metrics:
            rows.append({"route": route, "date": date_str, **metrics})
    return rows


def update_cr_fleet_table(current_date: date):
    rows = get_cr_fleet_metrics(current_date)
    print(f"Writing {len(rows)} Commuter Rail fleet rows for {current_date}")
    merge_rows(delivered_trip_metrics, rows)


if __name__ == "__main__":
    # Backfill, run from ingestor/:
    #   BACKFILL_START_DATE=2023-12-22 BACKFILL_END_DATE=2026-09-27 uv run python -m chalicelib.cr_fleet
    start = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
    end = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
    for d in range((end - start).days + 1):
        update_cr_fleet_table(start + timedelta(days=d))
