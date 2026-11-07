"""Backfill the HeadwayReliability table a month at a time. Run locally from ingestor/:

    BACKFILL_START_DATE=2016-01-01 BACKFILL_END_DATE=2026-11-06 uv run python -m chalicelib.headway_reliability.backfill

Archived months read each stop's monthly file once. With REFRESH_AWS_CREDENTIALS=1 it re-exports
`aws login` credentials before every month, since they only last 15 minutes.
"""

import json
import os
import subprocess
from datetime import date, timedelta

import boto3

from . import events
from .ingest import update_days


def refresh_credentials():
    exported = subprocess.run(
        ["aws", "configure", "export-credentials", "--format", "process"], capture_output=True, check=True, text=True
    )
    creds = json.loads(exported.stdout)
    boto3.setup_default_session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds.get("SessionToken"),
        region_name="us-east-1",
    )
    events.set_session(boto3.DEFAULT_SESSION)


def month_ends(start: date, end: date):
    while start <= end:
        next_month = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield start, min(end, next_month - timedelta(days=1))
        start = next_month


if __name__ == "__main__":
    start = date.fromisoformat(os.environ["BACKFILL_START_DATE"])
    end = date.fromisoformat(os.environ["BACKFILL_END_DATE"])
    for month_start, month_end in month_ends(start, end):
        if os.environ.get("REFRESH_AWS_CREDENTIALS"):
            refresh_credentials()
        update_days(month_start, month_end)
