"""Recompute fleet metrics on existing DeliveredTripMetrics rows (speed fields untouched),
then rebuild the weekly and monthly tables. Run from ingestor/:
    BACKFILL_START_DATE=2018-12-01 BACKFILL_END_DATE=2026-09-23 BACKFILL_LINE=line-green \
        uv run python -m chalicelib.trip_metrics.backfill_fleet
"""

import os
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

from .. import agg_speed_tables, constants
from ..car_ages import get_fleet_age_metrics_for_line

START_DATE = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
END_DATE = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
BACKFILL_LINE = os.environ.get("BACKFILL_LINE")  # e.g. "line-green", defaults to all lines

lines = [BACKFILL_LINE] if BACKFILL_LINE else constants.LINES
table = boto3.resource("dynamodb").Table("DeliveredTripMetrics")


def update_fleet_attributes(route: str, date_str: str, metrics: dict) -> bool:
    """Set fleet attributes on an existing row. Returns False if there's no row for route/date."""
    names = {f"#a{i}": key for i, key in enumerate(metrics)}
    values = {f":v{i}": value for i, value in enumerate(metrics.values())}
    try:
        table.update_item(
            Key={"route": route, "date": date_str},
            UpdateExpression="SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(metrics))),
            # Don't create sparse rows for days with no trip metrics
            ConditionExpression="attribute_exists(#r)",
            ExpressionAttributeNames={**names, "#r": "route"},
            ExpressionAttributeValues=values,
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


if __name__ == "__main__":
    num_days = (END_DATE - START_DATE).days + 1
    for line in lines:
        routes = constants.LINE_TO_ROUTE_MAP[line]
        for d in (progress := tqdm(range(num_days), desc=line)):
            current_date = START_DATE + timedelta(days=d)
            date_str = current_date.strftime(constants.DATE_FORMAT_BACKEND)
            progress.set_postfix_str(date_str)
            metrics = get_fleet_age_metrics_for_line(current_date, line)
            if not metrics:
                continue
            for route in routes:
                update_fleet_attributes(route, date_str, metrics)

    start_str = START_DATE.strftime(constants.DATE_FORMAT_BACKEND)
    for line in tqdm(lines, desc="Rebuilding weekly/monthly aggregates..."):
        agg_speed_tables.populate_table(line, "weekly", start_str)
        agg_speed_tables.populate_table(line, "monthly", start_str)
