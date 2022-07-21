import json

from chalicelib import MbtaPerformanceAPI, s3

BUCKET = "tm-mbta-performance"

def key(day):
    return f"ingestTest/Alerts/{str(day)}.json.gz"

def store_alerts(day):
    api_data = MbtaPerformanceAPI.get_api_data("pastalerts", {}, day)
    alerts = json.dumps(api_data).encode("utf8")
    s3.upload(BUCKET, key(day), alerts, True)
