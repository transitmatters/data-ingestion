import csv
import gzip
import io
from datetime import date

import boto3
from botocore.exceptions import ClientError

from .car_ages import _parse_car_id

EVENTS_BUCKET = "tm-mbta-performance"

s3 = boto3.client("s3")


def read_events(key: str) -> list[dict] | None:
    """Rows of an events CSV in the performance bucket, or None if the file doesn't exist."""
    try:
        body = s3.get_object(Bucket=EVENTS_BUCKET, Key=key)["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    if key.endswith(".gz"):
        body = gzip.decompress(body)
    return list(csv.DictReader(io.StringIO(body.decode("utf-8"))))


def vehicle_ids_at_stop(stop_id: str, current_date: date, key_templates: list[str]) -> list[int]:
    """One vehicle number per trip at a stop, from the first source that has them for that day."""
    for template in key_templates:
        key = template.format(stop=stop_id, year=current_date.year, month=current_date.month, day=current_date.day)
        vehicle_by_trip: dict[str, int] = {}
        for row in read_events(key) or []:
            vehicle_id = _parse_car_id(row.get("vehicle_label") or "")
            if vehicle_id is not None:
                vehicle_by_trip.setdefault(row["trip_id"], vehicle_id)
        if vehicle_by_trip:
            return list(vehicle_by_trip.values())
    return []


def merge_rows(table, rows: list[dict]):
    """Set each row's fields on its route/date item, keeping any other fields already there."""
    for row in rows:
        fields = {k: v for k, v in row.items() if k not in ("route", "date")}
        table.update_item(
            Key={"route": row["route"], "date": row["date"]},
            UpdateExpression="SET " + ", ".join(f"#a{i} = :v{i}" for i in range(len(fields))),
            ExpressionAttributeNames={f"#a{i}": k for i, k in enumerate(fields)},
            ExpressionAttributeValues={f":v{i}": v for i, v in enumerate(fields.values())},
        )
