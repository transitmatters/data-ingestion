"""Rescale miles_covered and track_mileage on existing DeliveredTripMetrics rows to the current
excluding_terminals lengths (count and total_time untouched), then rebuild the weekly and monthly tables.
Dry run by default, which prints the monthly peak speed per line before and after. Run from ingestor/:
    uv run python -m chalicelib.trip_metrics.backfill_track_mileage
    BACKFILL_WRITE=1 uv run python -m chalicelib.trip_metrics.backfill_track_mileage
"""

import os
from collections import defaultdict
from datetime import date
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from tqdm import tqdm

from .. import agg_speed_tables, constants

WRITE = os.environ.get("BACKFILL_WRITE") == "1"
BACKFILL_LINE = os.environ.get("BACKFILL_LINE")  # e.g. "line-green", defaults to all lines

lines = [BACKFILL_LINE] if BACKFILL_LINE else constants.LINES
table = boto3.resource("dynamodb").Table("DeliveredTripMetrics")


def query_route(route_key: str) -> list[dict]:
    items = []
    kwargs = {"KeyConditionExpression": Key("route").eq(route_key)}
    while True:
        response = table.query(**kwargs)
        items.extend(response["Items"])
        if "LastEvaluatedKey" not in response:
            return items
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def corrected_length(line: str, route_key: str, date_str: str) -> Decimal:
    branch = route_key.removeprefix(line).removeprefix("-") or None
    return constants.get_route_metadata(line, date.fromisoformat(date_str), False, branch)["length"]


def peak_monthly_mph(monthly: dict[str, list[Decimal]], index: int) -> tuple[float, str]:
    mph = {month: float(totals[index] / (totals[2] / 3600)) for month, totals in monthly.items() if totals[2] > 0}
    month = max(mph, key=mph.get)
    return round(mph[month], 1), month


if __name__ == "__main__":
    for line in lines:
        route_keys = constants.LINE_TO_ROUTE_MAP[line]
        updated = []
        # date => route => (old miles, new miles, total_time)
        by_date: dict[str, dict[str, tuple[Decimal, Decimal, Decimal]]] = defaultdict(dict)
        for route_key in tqdm(route_keys, desc=line):
            for item in query_route(route_key):
                if item.get("miles_covered") is None or item.get("count") is None:
                    continue
                length = corrected_length(line, route_key, item["date"])
                old_miles = item["miles_covered"]
                item["miles_covered"] = item["count"] * length
                item["track_mileage"] = length
                updated.append(item)
                by_date[item["date"]][route_key] = (old_miles, item["miles_covered"], item["total_time"])

        # Like agg_speed_tables, skip days where any branch is missing
        monthly: dict[str, list[Decimal]] = defaultdict(lambda: [Decimal(0)] * 3)
        for date_str, routes in by_date.items():
            if len(routes) < len(route_keys):
                continue
            totals = monthly[date_str[:7]]
            for values in routes.values():
                for i, value in enumerate(values):
                    totals[i] += value
        print(
            f"{line}: {len(updated)} rows, peak monthly mph {peak_monthly_mph(monthly, 0)} -> {peak_monthly_mph(monthly, 1)}"
        )

        if WRITE:
            with table.batch_writer() as batch:
                for item in updated:
                    batch.put_item(Item=item)

    if WRITE:
        for line in tqdm(lines, desc="Rebuilding weekly/monthly aggregates..."):
            agg_speed_tables.populate_table(line, "weekly")
            agg_speed_tables.populate_table(line, "monthly")
