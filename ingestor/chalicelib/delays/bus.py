"""Bus delay alerts, read from the LAMP alert history.

Subway and Commuter Rail ask the dashboard's /api/alerts once per line per day. With
~150 bus routes that would be ~150 requests a day, so bus reads the LAMP day file
(already downloaded for live minutes, see duration.py) once and splits it by route.
It builds the same rows /api/alerts would return for each route.

Lines are raw GTFS route ids ("1", "741" for SL1, "708" for CT3), the ids the alerts
use. The dashboard's combined routes ("114/116/117") sum their ids.
"""

from .duration import DASHBOARD_EFFECTS
from .types import Alert

BUS_ROUTE_TYPE = 3

# All bus routes summed together, like "CommuterRail"
BUS_AGGREGATE = "Bus"


def bus_alerts_by_route(lamp_alerts: dict) -> dict[str, list[Alert]]:
    """/api/alerts-style rows for every bus route in a LAMP day file: one row per
    active period of each alert, keyed by every bus route the alert informs."""
    by_route: dict[str, list[Alert]] = {}
    for alert in lamp_alerts.values():
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
        for route in routes:
            by_route.setdefault(route, []).extend(rows)
    return by_route
