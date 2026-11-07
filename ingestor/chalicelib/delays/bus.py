"""Bus delay alerts, read from the LAMP alert history.

Subway and Commuter Rail ask the dashboard's /api/alerts once per line per day. With
~150 bus routes that would be ~150 requests a day, so bus reads the LAMP day file
(already downloaded for live minutes, see duration.py) once and splits it by route.
It builds the same rows /api/alerts would return for each route.

Lines are raw GTFS route ids ("1", "741" for SL1, "708" for CT3), the ids the alerts
use. The dashboard's combined routes ("114/116/117") get a row of their own, keyed by
their ids joined with commas ("114,116,117"), where an alert posted on several of the
routes counts once. Summing the single-route rows would count it once per route.
"""

from .duration import DASHBOARD_EFFECTS
from .types import Alert

BUS_ROUTE_TYPE = 3

# All bus routes together, each alert counted once
BUS_AGGREGATE = "Bus"

# The dashboard's combined bus routes, as GTFS route ids in the order of their label:
# the dashboard asks for "741,742,743,746" for "SL1/SL2/SL3/SLW". Keep in step with
# `BusRoute` in t-performance-dash's common/types/lines.ts. Its "-legacy" variants are
# left out, since the dashboard can't ask for their delays.
BUS_ROUTE_GROUPS: list[tuple[str, ...]] = [
    ("17", "19"),
    ("24", "27", "33"),
    ("40", "50"),
    ("52", "59"),
    ("61", "70", "170"),
    ("62", "76"),
    ("64", "68"),
    ("67", "79"),
    ("72", "74", "75"),
    ("78", "84"),
    ("104", "109"),
    ("114", "116", "117"),
    ("120", "121"),
    ("131", "132"),
    ("136", "137"),
    ("201", "202"),
    ("210", "211", "212"),
    ("214", "216"),
    ("217", "245"),
    ("220", "221", "222"),
    ("225", "226"),
    ("350", "351"),
    ("411", "430"),
    ("424", "450", "456"),
    ("426", "428"),
    ("434", "435", "436"),
    ("439", "441", "442"),
    ("501", "503"),
    ("502", "504"),
    ("505", "553", "554"),
    ("556", "558"),
    ("712", "713"),
    ("708", "171"),  # CT3/171
    ("741", "742", "743", "746"),  # SL1/SL2/SL3/SLW
    ("751", "749"),  # SL4/SL5
]


def line_routes(line: str) -> set[str]:
    """The route ids behind a bus line: {"39"} for "39", {"114", "116", "117"} for "114,116,117"."""
    return set(line.split(","))


def bus_alerts(lamp_alerts: dict) -> dict[str, tuple[set[str], list[Alert]]]:
    """Every bus alert in a LAMP day file, by alert id: the bus routes it informs, and
    its /api/alerts-style rows (one per active period)."""
    alerts = {}
    for alert_id, alert in lamp_alerts.items():
        attrs = alert["attributes"]
        if attrs.get("effect") not in DASHBOARD_EFFECTS:
            continue
        routes = {
            entity["route"]
            for entity in attrs.get("informed_entity", [])
            if entity.get("route_type") == BUS_ROUTE_TYPE and entity.get("route")
        }
        text = attrs.get("header") or attrs.get("short_header")
        if not routes or not text:
            continue
        rows = [
            Alert(valid_from=period.get("start"), valid_to=period.get("end"), text=text)
            for period in attrs.get("active_period", [])
        ]
        alerts[alert_id] = (routes, rows)
    return alerts


def bus_alerts_by_route(lamp_alerts: dict) -> dict[str, list[Alert]]:
    """/api/alerts-style rows for every bus route and combined route in a LAMP day file.

    Each alert is listed once under every route it informs, and once under every
    combined route that includes any of them.
    """
    by_route: dict[str, list[Alert]] = {}
    for routes, rows in bus_alerts(lamp_alerts).values():
        lines = routes | {",".join(group) for group in BUS_ROUTE_GROUPS if routes.intersection(group)}
        for line in lines:
            by_route.setdefault(line, []).extend(rows)
    return by_route
