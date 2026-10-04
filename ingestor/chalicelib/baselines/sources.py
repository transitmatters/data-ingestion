"""Read the full weekly history of each series used for historical baselines."""

import json
from datetime import date

import boto3
from boto3.dynamodb.conditions import Key

from .. import constants, s3

TRIP_METRICS_TABLE = "DeliveredTripMetricsWeekly"
RIDERSHIP_TABLE = "Ridership"
SERVICE_RIDERSHIP_BUCKET = "tm-service-ridership-dashboard"
SERVICE_RIDERSHIP_KEY = "latest.json"

CR_RIDERSHIP_IDS = [constants.commuter_rail_ridership_key(line) for line in constants.COMMUTER_RAIL_LINES]

Series = dict[str, list[tuple[date, float | None]]]


def _paginate(operation, **kwargs) -> list[dict]:
    items = []
    while True:
        response = operation(**kwargs)
        items.extend(response["Items"])
        if "LastEvaluatedKey" not in response:
            return items
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def get_trip_metrics_series() -> tuple[Series, Series]:
    """Weekly (speed mph, delivered round trips) per rapid transit line, full history."""
    table = boto3.resource("dynamodb").Table(TRIP_METRICS_TABLE)
    speed: Series = {}
    service: Series = {}
    for line in constants.LINES:
        items = _paginate(table.query, KeyConditionExpression=Key("line").eq(line))
        speed[line] = []
        service[line] = []
        for item in items:
            week = date.fromisoformat(item["date"])
            miles = float(item.get("miles_covered") or 0)
            seconds = float(item.get("total_time") or 0)
            speed[line].append((week, miles / (seconds / 3600) if miles > 0 and seconds > 0 else None))
            service[line].append((week, float(item["count"]) if item.get("count") is not None else None))
    return speed, service


def get_ridership_series() -> Series:
    """Weekly ridership for every lineId in the Ridership table, plus a summed commuter rail series.

    The table is small (~4 MB), so a paginated scan is cheaper and simpler than enumerating keys.
    """
    table = boto3.resource("dynamodb").Table(RIDERSHIP_TABLE)
    items = _paginate(
        table.scan,
        ProjectionExpression="lineId, #date, #count",
        ExpressionAttributeNames={"#date": "date", "#count": "count"},
    )
    series: Series = {}
    for item in items:
        series.setdefault(item["lineId"], []).append((date.fromisoformat(item["date"]), float(item["count"])))

    commuter_rail: dict[date, float] = {}
    for line_id in CR_RIDERSHIP_IDS:
        for day, count in series.get(line_id, []):
            commuter_rail[day] = commuter_rail.get(day, 0) + count
    if commuter_rail:
        series["line-commuter-rail"] = sorted(commuter_rail.items())
    return series


def get_scheduled_service_series() -> Series:
    """Weekly scheduled trips per line and per mode, from the service & ridership dashboard.

    That job already resamples ScheduledServiceDaily (~135 MB) into weekly history, so reading its
    output is far cheaper than re-scanning the table.
    """
    dashboard = json.loads(s3.download(SERVICE_RIDERSHIP_BUCKET, SERVICE_RIDERSHIP_KEY, compressed=False))

    def to_points(history: dict[str, float | None]) -> list[tuple[date, float | None]]:
        return [(date.fromisoformat(day), value) for day, value in history.items()]

    series: Series = {"total": to_points(dashboard["summaryData"]["totalServiceHistory"])}
    for mode, mode_data in dashboard["modeData"].items():
        series[f"mode-{mode}"] = to_points(mode_data["totalServiceHistory"])
    for line_id, line_data in dashboard["lineData"].items():
        series[line_id] = to_points(line_data.get("serviceHistory") or {})
    return series
