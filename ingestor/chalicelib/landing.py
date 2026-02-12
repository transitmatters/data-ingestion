import json

import boto3
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as ddb_json

from chalicelib import constants, s3

dynamodb = boto3.resource("dynamodb")

BUCKETS = [
    "dashboard.transitmatters.org",
    "dashboard-beta.labs.transitmatters.org",
]

DISTRIBUTIONS = [
    "EH3F0Z8TUZVCQ",
    "E17EZQSPQV9OWI",
]  # dashboard  # dashboard-beta


TRIP_METRICS_KEY_JSON = "static/landing/trip_metrics.json"
RIDERSHIP_KEY_JSON = "static/landing/ridership.json"


def query_landing_trip_metrics_data(line: str):
    """Queries weekly trip metrics for a line over the last 90 days.

    Args:
        line: The line identifier (e.g. "line-red").

    Returns:
        A list of trip metric records for the landing page date range.
    """
    table = dynamodb.Table("DeliveredTripMetricsWeekly")
    response = table.query(
        KeyConditionExpression=Key("line").eq(line)
        & Key("date").between(constants.NINETY_DAYS_AGO_STRING, constants.ONE_WEEK_AGO_STRING)
    )
    return ddb_json.loads(response["Items"])


def get_trip_metrics_data():
    """Fetches weekly trip metrics for all lines for the landing page.

    Returns:
        A dict mapping line IDs to their trip metrics data.
    """
    trip_metrics_object = {}
    for line in constants.LINES:
        data = query_landing_trip_metrics_data(line)
        trip_metrics_object[line] = data
    return trip_metrics_object


def get_ridership_data():
    """Fetches weekly ridership data for all lines and commuter rail.

    Aggregates commuter rail lines into a single "line-commuter-rail" entry.

    Returns:
        A dict mapping line IDs to their ridership data.
    """
    ridership_object = {}
    for line in constants.LINES:
        data = query_landing_ridership_data(constants.RIDERSHIP_KEYS[line])
        ridership_object[line] = data

    # get data for commuter rail (treated as one line)
    ridership_object["line-commuter-rail"] = [None] * 11
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
    # filter out None values
    ridership_object["line-commuter-rail"] = [x for x in ridership_object["line-commuter-rail"] if x is not None]

    return ridership_object


def query_landing_ridership_data(line: str):
    """Queries ridership data for a line over the last 90 days.

    Args:
        line: The ridership line key (e.g. "line-Red").

    Returns:
        A list of ridership records for the landing page date range.
    """
    table = dynamodb.Table("Ridership")
    response = table.query(
        KeyConditionExpression=Key("lineId").eq(line)
        & Key("date").between(constants.NINETY_DAYS_AGO_STRING, constants.ONE_WEEK_AGO_STRING)
    )
    return ddb_json.loads(response["Items"])


def upload_to_s3(trip_metrics, ridership):
    """Uploads trip metrics and ridership JSON to all dashboard S3 buckets.

    Args:
        trip_metrics: JSON string of trip metrics data.
        ridership: JSON string of ridership data.
    """
    for bucket in BUCKETS:
        print(f"Uploading to {bucket}")
        s3.upload(bucket, RIDERSHIP_KEY_JSON, ridership, compress=False)
        s3.upload(bucket, TRIP_METRICS_KEY_JSON, trip_metrics, compress=False)


def clear_cache():
    """Invalidates the CloudFront cache for landing page data across all distributions."""
    for distribution in DISTRIBUTIONS:
        s3.clear_cf_cache(distribution, ["/static/landing/*"])


if __name__ == "__main__":
    print(
        f"Uploading ridership and trip metric data for landing page from {constants.NINETY_DAYS_AGO_STRING} to {constants.ONE_WEEK_AGO_STRING}"
    )
    trip_metrics_data = get_trip_metrics_data()
    ridership_data = get_ridership_data()
    upload_to_s3(json.dumps(trip_metrics_data), json.dumps(ridership_data))
    clear_cache()
