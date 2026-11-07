"""Inputs to the home page scoreboard: the same files the dashboard used to read in the browser,
plus daily bus fleet rows."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Key

from .. import landing, s3
from ..baselines.sources import SERVICE_RIDERSHIP_BUCKET, SERVICE_RIDERSHIP_KEY, _paginate
from ..headway_reliability.buckets import HEADWAY_FIELDS
from .config import FREQUENT_BUS_LINE_IDS, ROWS

DASHBOARD_BUCKET = landing.BUCKETS[0]
TRIP_METRICS_KEY = landing.TRIP_METRICS_KEY_JSON
BASELINES_KEY = "static/landing/baselines.json"
DELAY_TOTALS_KEY = "static/slowzones/delay_totals.json"

BUS_FLEET_TABLE = "DeliveredTripMetricsBus"
# The system-wide row in DeliveredTripMetricsBus.
SYSTEM_WIDE_ROUTE = "all"
BUS_FLEET_FIELDS = ["fleet_trips", "fleet_mix_battery", "fleet_mix_hybrid", "avg_bus_age"]
BUS_FLEET_WEEKS = 13

SUBWAY_HEADWAYS_TABLE = "HeadwayReliability"
HEADWAY_WEEKS = 13
SUBWAY_LINES = [row["line"] for row in ROWS if row["id"] in ("red", "orange", "blue", "green", "mattapan")]

# File names for --inputs.
LOCAL_FILES = {
    "trip_metrics": "trip_metrics.json",
    "baselines": "baselines.json",
    "delay_totals": "delay_totals.json",
    "service_ridership": "service_ridership_dashboard.json",
    "bus_fleet": "bus_fleet.json",
    "subway_headways": "subway_headways.json",
    "bus_headways": "bus_headways.json",
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
    # Daily headway bucket counts per subway line and per GTFS bus route; None when they couldn't be loaded.
    subway_headways: dict[str, list[dict]] | None = None
    bus_headways: dict[str, list[dict]] | None = None


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


def bus_routes(service_ridership: dict) -> list[str]:
    """Every GTFS route behind the bus and Silver Line lines."""
    return [
        route
        for line in service_ridership["lineData"].values()
        if line.get("lineKind") in ("bus", "silver")
        for route in line.get("routeIds", [])
    ]


def _query_daily(table_name: str, routes: list[str], fields: list[str], weeks: int, now: datetime):
    table = boto3.resource("dynamodb").Table(table_name)
    start = (now - timedelta(weeks=weeks)).date().isoformat()
    end = (now - timedelta(days=1)).date().isoformat()

    def query(route: str) -> list[dict]:
        return _plain(
            _paginate(
                table.query,
                KeyConditionExpression=Key("route").eq(route) & Key("date").between(start, end),
                ProjectionExpression=", ".join(["#date", *fields]),
                ExpressionAttributeNames={"#date": "date"},
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        return dict(zip(routes, pool.map(query, routes)))


def get_bus_fleet(routes: list[str], now: datetime) -> dict[str, list[dict]]:
    return _query_daily(BUS_FLEET_TABLE, routes, BUS_FLEET_FIELDS, BUS_FLEET_WEEKS, now)


def get_headways(table_name: str, routes: list[str], now: datetime) -> dict[str, list[dict]] | None:
    try:
        return _query_daily(table_name, routes, HEADWAY_FIELDS, HEADWAY_WEEKS, now)
    except Exception as e:  # the scoreboard still stands without reliability cells
        print(f"Couldn't load headway rows from {table_name}: {e}")
        return None


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
        subway_headways=get_headways(SUBWAY_HEADWAYS_TABLE, SUBWAY_LINES, now),
        bus_headways=get_headways(BUS_FLEET_TABLE, bus_routes(service_ridership), now),
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
        subway_headways=read("subway_headways"),
        bus_headways=read("bus_headways"),
    )
