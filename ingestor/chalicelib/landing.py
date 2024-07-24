import boto3
from boto3.dynamodb.conditions import Key
from chalicelib import s3
from dynamodb_json import json_util as ddb_json
from chalicelib import constants

dynamodb = boto3.resource("dynamodb")

BUCKETS = [
    "dashboard.transitmatters.org",
    "dashboard-beta.labs.transitmatters.org",
]

DISTRIBUTIONS = [
    "EH3F0Z8TUZVCQ",
    "ENTCATGUNQIQ",
]  # dashboard  # dashboard-beta


TRIP_METRICS_KEY_JSON = "static/landing/trip_metrics.json"
RIDERSHIP_KEY_JSON = "static/landing/ridership.json"


def query_landing_trip_metrics_data(line: str):
    table = dynamodb.Table("DeliveredTripMetricsWeekly")
    response = table.query(
        KeyConditionExpression=Key("line").eq(line)
        & Key("date").between(constants.NINETY_DAYS_AGO_STRING, constants.ONE_WEEK_AGO_STRING)
    )
    return ddb_json.loads(response["Items"])


def get_trip_metrics_data():
    trip_metrics_object = {}
    for line in constants.LINES:
        data = query_landing_trip_metrics_data(line)
        trip_metrics_object[line] = data
    return trip_metrics_object


def get_ridership_data():
    ridership_object = {}
    for line in constants.LINES:
        data = query_landing_ridership_data(constants.RIDERSHIP_KEYS[line])
        ridership_object[line] = data

    ridership_object["line-commuter-rail"] = [None] * 10
    for line in constants.COMMUTER_RAIL_LINES:
        data = query_landing_ridership_data(constants.commuter_rail_ridership_key(line))
        for index, week in enumerate(data):
            if ridership_object["line-commuter-rail"][index] is None:
                ridership_object["line-commuter-rail"][index] = week
                continue
            else:
                data = {
                    "lineId": "line-commuter-rail",
                    "count": ridership_object["line-commuter-rail"][index]["count"] + week["count"],
                    "timestamp": week["timestamp"],
                    "date": week["date"],
                }
                ridership_object["line-commuter-rail"][index] = data

    return ridership_object


def query_landing_ridership_data(line: str):
    table = dynamodb.Table("Ridership")
    response = table.query(
        KeyConditionExpression=Key("lineId").eq(line)
        & Key("date").between(constants.NINETY_DAYS_AGO_STRING, constants.ONE_WEEK_AGO_STRING)
    )
    return ddb_json.loads(response["Items"])


def upload_to_s3(trip_metrics, ridership):
    for bucket in BUCKETS:
        print(f"Uploading to {bucket}")
        s3.upload(bucket, RIDERSHIP_KEY_JSON, ridership, compress=False)
        s3.upload(bucket, TRIP_METRICS_KEY_JSON, trip_metrics, compress=False)


def clear_cache():
    for distribution in DISTRIBUTIONS:
        s3.clear_cf_cache(distribution, ["/static/landing/*"])
