import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

import requests

from . import constants, dynamo
from .car_ages import _parse_car_id, average_age, mix_percentages

TABLE_NAME = "BusFleetMetrics"
ALL_ROUTES_KEY = "all"

# Bus number range -> (build year, propulsion), from the NETransit MBTA roster (09/23/26).
# Extended-range hybrids count as hybrid. 4305-4387 are on order: add their build years on delivery.
BUS_FLEET: dict[str, tuple[float | None, str]] = {
    "0756-0910": (2008, "diesel"),
    "1200-1224": (2010, "hybrid"),
    "1250-1293": (2016.5, "hybrid"),
    "1294-1294": (2018, "hybrid"),
    "1295-1299": (2019, "battery"),
    "1300-1344": (2022.75, "hybrid"),
    "1400-1459": (2014.5, "hybrid"),
    "1600-1774": (2016.5, "cng"),
    "1775-1924": (2016.5, "hybrid"),
    "1925-2118": (2019.5, "hybrid"),
    "3000-3005": (2017, "hybrid"),
    "3100-3159": (2020, "hybrid"),
    "3200-3359": (2023, "hybrid"),
    "4200-4231": (2025, "battery"),
    "4300-4304": (2025, "battery"),
    "4305-4387": (None, "battery"),
}
PROPULSIONS = ["diesel", "hybrid", "cng", "battery"]

# Stops sampled per GTFS route: the busiest stop gobble captures in each direction, found by
# checking the headways API on recent weekdays.
BUS_FLEET_STOPS: dict[str, list[str]] = json.loads((Path(__file__).parent / "bus_fleet_stops.json").read_text())


def get_bus_info(bus_id: int) -> tuple[float | None, str] | None:
    """(build year, propulsion) for a bus number, or None if it isn't an MBTA revenue bus."""
    for range_str, info in BUS_FLEET.items():
        low, high = range_str.split("-")
        if int(low) <= bus_id <= int(high):
            return info
    return None


def compute_bus_metrics(trip_bus_ids: list[int], current_date: date) -> dict[str, Decimal]:
    """Fleet metrics for one bus sampled per trip. Buses outside BUS_FLEET are left out."""
    counts = {propulsion: 0 for propulsion in PROPULSIONS}
    build_years: dict[int, float] = {}
    for bus_id in trip_bus_ids:
        info = get_bus_info(bus_id)
        if info is None:
            continue
        year, propulsion = info
        counts[propulsion] += 1
        if year is not None:
            build_years[bus_id] = year

    trips = sum(counts.values())
    if not trips:
        return {}
    metrics: dict[str, Decimal] = {
        "trips": Decimal(trips),
        "pct_battery_trips": Decimal(str(round(counts["battery"] / trips * 100, 1))),
        **mix_percentages(counts),
    }
    if build_years:
        metrics["avg_bus_age"] = average_age(list(build_years.values()), current_date)
    return metrics


def _bus_ids_at_stop(stop_id: str, current_date: date) -> list[int]:
    url = constants.DD_URL_HEADWAYS.format(
        date=current_date.strftime(constants.DATE_FORMAT_BACKEND), parameters=urlencode({"stop": stop_id})
    )
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch headways for bus fleet ({stop_id}, {current_date}): {e}")
        return []
    bus_ids = []
    for departure in response.json():
        bus_id = _parse_car_id(str(departure.get("vehicle_label") or ""))
        if bus_id is not None:
            bus_ids.append(bus_id)
    return bus_ids


def _bus_ids_for_route(route: str, current_date: date) -> list[int]:
    return [bus_id for stop_id in BUS_FLEET_STOPS[route] for bus_id in _bus_ids_at_stop(stop_id, current_date)]


def get_bus_fleet_metrics(current_date: date) -> list[dict]:
    """One row per sampled route, plus an "all" row pooled across every route's trips."""
    with ThreadPoolExecutor(max_workers=8) as executor:
        route_bus_ids = dict(
            zip(BUS_FLEET_STOPS, executor.map(lambda r: _bus_ids_for_route(r, current_date), BUS_FLEET_STOPS))
        )
    date_str = current_date.strftime(constants.DATE_FORMAT_BACKEND)
    rows = []
    for route, bus_ids in [*route_bus_ids.items(), (ALL_ROUTES_KEY, sum(route_bus_ids.values(), []))]:
        metrics = compute_bus_metrics(bus_ids, current_date)
        if metrics:
            rows.append({"route": route, "date": date_str, **metrics})
    return rows


def update_bus_fleet_table(current_date: date):
    rows = get_bus_fleet_metrics(current_date)
    print(f"Writing {len(rows)} bus fleet rows for {current_date}")
    dynamo.dynamo_batch_write(rows, TABLE_NAME)


if __name__ == "__main__":
    # Backfill, run from ingestor/:
    #   BACKFILL_START_DATE=2024-01-01 BACKFILL_END_DATE=2026-09-24 uv run python -m chalicelib.bus_fleet
    start = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
    end = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
    for d in range((end - start).days + 1):
        update_bus_fleet_table(start + timedelta(days=d))
