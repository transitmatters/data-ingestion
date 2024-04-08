import json

from chalicelib import MbtaPerformanceAPI, s3

BUCKET = "tm-mbta-performance"


def key(day):
    return f"Alerts/{str(day)}.json.gz"


def store_alerts(day):
    # TODO: Replace with v3 API calls
    # V3 call only store same day and upcoming alerts, so we'll need to change how this runs
    # We may need to call it hourly during the service day to get all alerts
    api_data = MbtaPerformanceAPI.get_api_data("pastalerts", {}, day)
    alerts = json.dumps(api_data).encode("utf8")
    s3.upload(BUCKET, key(day), alerts, True)
