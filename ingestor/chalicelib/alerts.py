import json

import requests

from chalicelib import s3
from chalicelib.date_utils import get_current_service_date
from botocore.exceptions import ClientError

BUCKET = "tm-mbta-performance"


def key(day, v3: bool = False):
    if v3:
        return f"Alerts/v3/{str(day)}.json.gz"
    return f"Alerts/{str(day)}.json.gz"


def save_v3_alerts():
    r_s = requests.get("https://api-v3.mbta.com/alerts")
    alerts = r_s.json()

    service_date = get_current_service_date()
    try:
        current_alerts = s3.download(BUCKET, key(service_date, v3=True), encoding="utf8", compressed=True)
        all_alerts = json.loads(current_alerts)
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
        all_alerts = dict()

    for alert in alerts["data"]:
        all_alerts[alert["id"]] = alert

    alert_json = json.dumps(all_alerts).encode("utf8")
    s3.upload(BUCKET, key(service_date, v3=True), alert_json, compress=True)
