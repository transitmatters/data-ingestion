"""Inputs to the home page scoreboard: the same files the dashboard used to read in the browser,
plus daily bus fleet rows."""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Key

from .. import landing, s3
from ..baselines.sources import SERVICE_RIDERSHIP_BUCKET, SERVICE_RIDERSHIP_KEY, _paginate
from .config import FREQUENT_BUS_LINE_IDS

DASHBOARD_BUCKET = landing.BUCKETS[0]
TRIP_METRICS_KEY = landing.TRIP_METRICS_KEY_JSON
BASELINES_KEY = "static/landing/baselines.json"
DELAY_TOTALS_KEY = "static/slowzones/delay_totals.json"

BUS_FLEET_TABLE = "DeliveredTripMetricsBus"
# The system-wide row in DeliveredTripMetricsBus.
SYSTEM_WIDE_ROUTE = "all"
BUS_FLEET_FIELDS = ["fleet_trips", "fleet_mix_battery", "fleet_mix_hybrid", "avg_bus_age"]
BUS_FLEET_WEEKS = 13

# File names for --inputs.
LOCAL_FILES = {
    "trip_metrics": "trip_metrics.json",
    "baselines": "baselines.json",
    "delay_totals": "delay_totals.json",
    "service_ridership": "service_ridership_dashboard.json",
    "bus_fleet": "bus_fleet.json",
}


@dataclass
class Sources:
    # Weekly rows per rapid transit line, as in static/landing/trip_metrics.json.
    trip_metrics: dict[str, list[dict]]
    baselines: dict | None
    delay_totals: dict
    service_ridership: dict
    # Daily fleet rows per GTFS route id (and "all"); None when they couldn't be loaded.
    bus_fleet: dict[str, list[dict]] | None
    now: datetime


def _plain(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _plain(v) for key, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value


def bus_fleet_routes(service_ridership: dict) -> list[str]:
    """The system-wide row plus every GTFS route behind the Frequent Bus lines."""
    routes = [SYSTEM_WIDE_ROUTE]
    for line_id in FREQUENT_BUS_LINE_IDS:
        routes += service_ridership["lineData"].get(line_id, {}).get("routeIds", [])
    return routes


def get_bus_fleet(routes: list[str], now: datetime) -> dict[str, list[dict]]:
    table = boto3.resource("dynamodb").Table(BUS_FLEET_TABLE)
    start = (now - timedelta(weeks=BUS_FLEET_WEEKS)).date().isoformat()
    end = (now - timedelta(days=1)).date().isoformat()
    return {
        route: _plain(
            _paginate(
                table.query,
                KeyConditionExpression=Key("route").eq(route) & Key("date").between(start, end),
                ProjectionExpression=", ".join(["#date", *BUS_FLEET_FIELDS]),
                ExpressionAttributeNames={"#date": "date"},
            )
        )
        for route in routes
    }


def _download_json(bucket: str, key: str):
    return json.loads(s3.download(bucket, key, compressed=False))


def load(now: datetime) -> Sources:
    service_ridership = _download_json(SERVICE_RIDERSHIP_BUCKET, SERVICE_RIDERSHIP_KEY)
    try:
        bus_fleet = get_bus_fleet(bus_fleet_routes(service_ridership), now)
    except Exception as e:  # the scoreboard still stands without bus fleet cells
        print(f"Couldn't load bus fleet rows: {e}")
        bus_fleet = None
    return Sources(
        trip_metrics=_download_json(DASHBOARD_BUCKET, TRIP_METRICS_KEY),
        baselines=_download_json(DASHBOARD_BUCKET, BASELINES_KEY),
        delay_totals=_download_json(DASHBOARD_BUCKET, DELAY_TOTALS_KEY),
        service_ridership=service_ridership,
        bus_fleet=bus_fleet,
        now=now,
    )


def load_local(directory: str, now: datetime) -> Sources:
    def read(name: str):
        path = Path(directory) / LOCAL_FILES[name]
        return json.loads(path.read_text()) if path.exists() else None

    return Sources(
        trip_metrics=read("trip_metrics") or {},
        baselines=read("baselines"),
        delay_totals=read("delay_totals") or {"data": []},
        service_ridership=read("service_ridership"),
        bus_fleet=read("bus_fleet"),
        now=now,
    )
