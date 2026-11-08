"""Inputs to the home page scoreboard: the same files the dashboard used to read in the browser,
plus daily bus fleet rows."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import boto3
import requests
from boto3.dynamodb.conditions import Key

from .. import keys, landing, s3
from ..baselines.sources import SERVICE_RIDERSHIP_BUCKET, SERVICE_RIDERSHIP_KEY, _paginate
from ..delays.process import DAILY_TABLE_NAME as ALERT_DELAYS_TABLE
from ..headway_reliability.buckets import HEADWAY_FIELDS
from .config import FALLBACK_FREQUENT_BUS_LINE_IDS, ROWS
from .delays import SUBWAY_DELAY_LINES, bus_delay_lines
from .delivered import BUS_TRIP_FIELDS, EXTENDED_FIELDS, SCHEDULED_FIELDS, SUBWAY_ROUTES

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
CR_RELIABILITY_TABLE = "Reliability"
CR_RELIABILITY_FIELDS = ["otpNumerator", "otpDenominator", "cancelled", "peak"]
# Long enough to reach back past the gap between the MBTA's batches.
CR_RELIABILITY_WEEKS = 30
ALERT_DELAYS_FIELDS = ["delay_alert_minutes"]
ALERT_DELAYS_WEEKS = 13
EXTENDED_TRIP_METRICS_TABLE = "DeliveredTripMetricsExtended"
SCHEDULED_SERVICE_TABLE = "ScheduledServiceDaily"
# Enough to cover the delivered share's 4 complete weeks.
DELIVERED_WEEKS = 5

MBTA_ROUTES_URL = "https://api-v3.mbta.com/routes"
FREQUENT_BUS_DESCRIPTION = "Frequent Bus"

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
    "cr_reliability": "cr_reliability.json",
    "delays": "delays.json",
    "subway_delivered": "subway_delivered.json",
    "subway_scheduled": "subway_scheduled.json",
    "frequent_bus_routes": "frequent_bus_routes.json",
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
    # Daily on-time counts from every Commuter Rail line, pooled; None when they couldn't be loaded.
    cr_reliability: list[dict] | None = None
    # Daily alert delay rows per AlertDelaysDaily line; None when they couldn't be loaded.
    delays: dict[str, list[dict]] | None = None
    # Daily DeliveredTripMetricsExtended rows (all of a line's routes) and ScheduledServiceDaily rows
    # per subway line; None when they couldn't be loaded.
    subway_delivered: dict[str, list[dict]] | None = None
    subway_scheduled: dict[str, list[dict]] | None = None
    # Service & ridership line ids in the Frequent Bus row.
    frequent_bus_line_ids: list[str] = field(default_factory=lambda: list(FALLBACK_FREQUENT_BUS_LINE_IDS))


def _plain(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _plain(v) for key, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value


def get_frequent_bus_routes() -> list[str] | None:
    """GTFS route ids the MBTA describes as Frequent Bus, or None if its API can't be reached."""
    try:
        response = requests.get(
            MBTA_ROUTES_URL,
            params={"filter[type]": 3, "fields[route]": "description"},
            headers={"x-api-key": keys.MBTA_V3_API_KEY} if keys.MBTA_V3_API_KEY else {},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Couldn't load Frequent Bus routes from the MBTA: {e}")
        return None
    routes = [
        route["id"]
        for route in response.json()["data"]
        if route["attributes"].get("description") == FREQUENT_BUS_DESCRIPTION
    ]
    return routes or None


def frequent_bus_line_ids(service_ridership: dict, routes: list[str] | None) -> list[str]:
    """Service & ridership bus lines with any of `routes`, or the fallback list without them."""
    if not routes:
        return list(FALLBACK_FREQUENT_BUS_LINE_IDS)
    return [
        line_id
        for line_id, line in service_ridership["lineData"].items()
        if line.get("lineKind") in ("bus", "silver") and set(routes).intersection(line.get("routeIds", []))
    ]


def bus_fleet_routes(service_ridership: dict, line_ids: list[str]) -> list[str]:
    """The system-wide row plus every GTFS route behind the Frequent Bus lines."""
    routes = [SYSTEM_WIDE_ROUTE]
    for line_id in line_ids:
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


def cr_routes(service_ridership: dict) -> list[str]:
    """Every GTFS route behind the Commuter Rail lines."""
    return [
        route
        for line in service_ridership["lineData"].values()
        if line.get("lineKind") == "regional-rail"
        for route in line.get("routeIds", [])
    ]


def _query_daily(table_name: str, routes: list[str], fields: list[str], weeks: int, now: datetime, key: str = "route"):
    table = boto3.resource("dynamodb").Table(table_name)
    start = (now - timedelta(weeks=weeks)).date().isoformat()
    end = (now - timedelta(days=1)).date().isoformat()

    def query(route: str) -> list[dict]:
        return _plain(
            _paginate(
                table.query,
                KeyConditionExpression=Key(key).eq(route) & Key("date").between(start, end),
                ProjectionExpression=", ".join(["#date", *fields]),
                ExpressionAttributeNames={"#date": "date"},
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        return dict(zip(routes, pool.map(query, routes)))


def get_bus_fleet(routes: list[str], now: datetime) -> dict[str, list[dict]]:
    return _query_daily(BUS_FLEET_TABLE, routes, BUS_FLEET_FIELDS, BUS_FLEET_WEEKS, now)


def get_headways(
    table_name: str, routes: list[str], now: datetime, extra_fields: list[str] | None = None
) -> dict[str, list[dict]] | None:
    try:
        return _query_daily(table_name, routes, [*HEADWAY_FIELDS, *(extra_fields or [])], HEADWAY_WEEKS, now)
    except Exception as e:  # the scoreboard still stands without reliability cells
        print(f"Couldn't load headway rows from {table_name}: {e}")
        return None


def get_cr_reliability(routes: list[str], now: datetime) -> list[dict] | None:
    try:
        by_route = _query_daily(
            CR_RELIABILITY_TABLE, routes, CR_RELIABILITY_FIELDS, CR_RELIABILITY_WEEKS, now, key="routeId"
        )
    except Exception as e:  # the scoreboard still stands without the Commuter Rail reliability cell
        print(f"Couldn't load Commuter Rail on-time rows: {e}")
        return None
    return [row for rows in by_route.values() for row in rows]


def get_delays(service_ridership: dict, now: datetime) -> dict[str, list[dict]] | None:
    bus_lines = [line for line in service_ridership["lineData"].values() if line.get("lineKind") in ("bus", "silver")]
    lines = [*SUBWAY_DELAY_LINES.values(), *bus_delay_lines(bus_lines)]
    try:
        return _query_daily(ALERT_DELAYS_TABLE, lines, ALERT_DELAYS_FIELDS, ALERT_DELAYS_WEEKS, now, key="line")
    except Exception as e:  # the scoreboard still stands without delay cells
        print(f"Couldn't load alert delay rows: {e}")
        return None


def get_subway_delivered(now: datetime) -> tuple[dict[str, list[dict]] | None, dict[str, list[dict]] | None]:
    try:
        routes = [route for routes, _ in SUBWAY_ROUTES.values() for route in routes]
        by_route = _query_daily(EXTENDED_TRIP_METRICS_TABLE, routes, EXTENDED_FIELDS, DELIVERED_WEEKS, now)
        scheduled_ids = [scheduled_id for _, scheduled_id in SUBWAY_ROUTES.values()]
        by_id = _query_daily(
            SCHEDULED_SERVICE_TABLE, scheduled_ids, SCHEDULED_FIELDS, DELIVERED_WEEKS, now, key="routeId"
        )
    except Exception as e:  # the Service cells still stand without the delivered share
        print(f"Couldn't load delivered and scheduled service hours: {e}")
        return None, None
    delivered = {
        line: [row for route in routes for row in by_route[route]] for line, (routes, _) in SUBWAY_ROUTES.items()
    }
    scheduled = {line: by_id[scheduled_id] for line, (_, scheduled_id) in SUBWAY_ROUTES.items()}
    return delivered, scheduled


def _download_json(bucket: str, key: str):
    return json.loads(s3.download(bucket, key, compressed=False))


def load(now: datetime) -> Sources:
    service_ridership = _download_json(SERVICE_RIDERSHIP_BUCKET, SERVICE_RIDERSHIP_KEY)
    frequent_line_ids = frequent_bus_line_ids(service_ridership, get_frequent_bus_routes())
    try:
        bus_fleet = get_bus_fleet(bus_fleet_routes(service_ridership, frequent_line_ids), now)
    except Exception as e:  # the scoreboard still stands without bus fleet cells
        print(f"Couldn't load bus fleet rows: {e}")
        bus_fleet = None
    subway_delivered, subway_scheduled = get_subway_delivered(now)
    return Sources(
        trip_metrics=_download_json(DASHBOARD_BUCKET, TRIP_METRICS_KEY),
        baselines=_download_json(DASHBOARD_BUCKET, BASELINES_KEY),
        delay_totals=_download_json(DASHBOARD_BUCKET, DELAY_TOTALS_KEY),
        service_ridership=service_ridership,
        bus_fleet=bus_fleet,
        now=now,
        subway_headways=get_headways(SUBWAY_HEADWAYS_TABLE, SUBWAY_LINES, now),
        bus_headways=get_headways(BUS_FLEET_TABLE, bus_routes(service_ridership), now, BUS_TRIP_FIELDS),
        cr_reliability=get_cr_reliability(cr_routes(service_ridership), now),
        delays=get_delays(service_ridership, now),
        subway_delivered=subway_delivered,
        subway_scheduled=subway_scheduled,
        frequent_bus_line_ids=frequent_line_ids,
    )


def load_local(directory: str, now: datetime) -> Sources:
    def read(name: str):
        path = Path(directory) / LOCAL_FILES[name]
        return json.loads(path.read_text()) if path.exists() else None

    service_ridership = read("service_ridership")
    return Sources(
        trip_metrics=read("trip_metrics") or {},
        baselines=read("baselines"),
        delay_totals=read("delay_totals") or {"data": []},
        service_ridership=service_ridership,
        bus_fleet=read("bus_fleet"),
        now=now,
        subway_headways=read("subway_headways"),
        bus_headways=read("bus_headways"),
        cr_reliability=read("cr_reliability"),
        delays=read("delays"),
        subway_delivered=read("subway_delivered"),
        subway_scheduled=read("subway_scheduled"),
        frequent_bus_line_ids=frequent_bus_line_ids(service_ridership, read("frequent_bus_routes")),
    )
