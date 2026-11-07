"""Reading per-stop subway events from the performance bucket. Key layouts match the dashboard's
server/chalicelib/s3.py; months and days aren't zero-padded."""

import io
from datetime import date

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from .compute import COLUMNS

EVENTS_BUCKET = "tm-mbta-performance"

# The MBTA's monthly archives come first, then LAMP, then gobble.
MONTHLY = "Events/monthly-data/{stop}/Year={year}/Month={month}/events.csv.gz"
DAILY = [
    ("lamp", "Events-lamp/daily-data/{stop}/Year={year}/Month={month}/Day={day}/events.csv"),
    ("gobble", "Events-live/daily-rapid-data/{stop}/Year={year}/Month={month}/Day={day}/events.csv.gz"),
]

# A stop that's in every archived month, to tell whether a month has been archived yet.
SENTINEL_STOP = "70063"  # Davis, southbound

s3 = boto3.client("s3")


def set_session(session: boto3.Session):
    """Switch to fresh credentials during a long local backfill."""
    global s3
    s3 = session.client("s3")


def key_for(template: str, stop: str, day: date) -> str:
    return template.format(stop=stop, year=day.year, month=day.month, day=day.day)


def read_events(key: str) -> pd.DataFrame | None:
    """The columns headway reliability needs from one events file, or None if it doesn't exist."""
    try:
        body = s3.get_object(Bucket=EVENTS_BUCKET, Key=key)["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    df = pd.read_csv(
        io.BytesIO(body),
        compression="gzip" if key.endswith(".gz") else None,
        usecols=lambda c: c in COLUMNS,
        dtype=str,
    )
    if "scheduled_headway" not in df:
        df["scheduled_headway"] = None
    return df


def month_archived(day: date) -> bool:
    try:
        s3.head_object(Bucket=EVENTS_BUCKET, Key=key_for(MONTHLY, SENTINEL_STOP, day))
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise
