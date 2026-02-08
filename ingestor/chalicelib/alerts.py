import json

import requests
from botocore.exceptions import ClientError

from chalicelib import s3
from chalicelib.date_utils import get_current_service_date

BUCKET = "tm-mbta-performance"


def key(day):
    """Builds the S3 key for a day's alert data.

    Args:
        day: The service date.

    Returns:
        The S3 key string for the compressed alerts JSON.
    """
    return f"Alerts/v3/{str(day)}.json.gz"


def save_v3_alerts():
    """Fetches current MBTA V3 alerts and appends them to today's alert file in S3.

    Downloads the existing alert data for the current service date (if any),
    merges in newly fetched alerts by ID, and uploads the updated set back
    to S3.
    """
    r_s = requests.get("https://api-v3.mbta.com/alerts")
    alerts = r_s.json()

    service_date = get_current_service_date()
    try:
        current_alerts = s3.download(BUCKET, key(service_date), encoding="utf8", compressed=True)
        all_alerts = json.loads(current_alerts)
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
        all_alerts = dict()

    for alert in alerts["data"]:
        all_alerts[alert["id"]] = alert

    alert_json = json.dumps(all_alerts).encode("utf8")
    s3.upload(BUCKET, key(service_date), alert_json, compress=True)
