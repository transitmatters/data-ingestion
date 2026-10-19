import json

import requests
from botocore.exceptions import ClientError

from chalicelib import keys, s3
from chalicelib.date_utils import get_current_service_date

BUCKET = "tm-mbta-performance"


def key(day):
    return f"Alerts/v3/{str(day)}.json.gz"


def save_v3_alerts():
    headers = {"x-api-key": keys.MBTA_V3_API_KEY} if keys.MBTA_V3_API_KEY else {}
    r_s = requests.get("https://api-v3.mbta.com/alerts", headers=headers)
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
