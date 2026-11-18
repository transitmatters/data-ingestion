import json
from datetime import date, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as ddb_json

from . import constants, s3

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


def landing_date_range(today: date | None = None) -> tuple[str, str]:
    """The landing page window, 90 days ago through one week ago, as YYYY-MM-DD strings.

    Computed per call rather than at import: a container kept warm past midnight
    would otherwise keep using the previous day's window.
    """
    today = today or date.today()
    start = today - timedelta(days=90)
    end = today - timedelta(weeks=1)
    return start.strftime(constants.DATE_FORMAT_BACKEND), end.strftime(constants.DATE_FORMAT_BACKEND)


def query_landing_trip_metrics_data(line: str, start: str, end: str):
    """Queries weekly trip metrics for a line between two dates.

    Args:
        line: The line identifier (e.g. "line-red").
        start: First date, YYYY-MM-DD.
        end: Last date, YYYY-MM-DD.

    Returns:
        A list of trip metric records for the landing page date range.
    """
    table = dynamodb.Table("DeliveredTripMetricsWeekly")
    response = table.query(KeyConditionExpression=Key("line").eq(line) & Key("date").between(start, end))
    return ddb_json.loads(response["Items"])


def get_trip_metrics_data():
    """Fetches weekly trip metrics for all lines for the landing page.

    Returns:
        A dict mapping line IDs to their trip metrics data.
    """
    start, end = landing_date_range()
    trip_metrics_object = {}
    for line in constants.LINES:
        data = query_landing_trip_metrics_data(line, start, end)
        trip_metrics_object[line] = data
    return trip_metrics_object


def get_ridership_data():
    """Fetches weekly ridership data for all lines and commuter rail.

    Aggregates commuter rail lines into a single "line-commuter-rail" entry.

    Returns:
        A dict mapping line IDs to their ridership data.
    """
    start, end = landing_date_range()
    ridership_object = {}
    for line in constants.LINES:
        data = query_landing_ridership_data(constants.RIDERSHIP_KEYS[line], start, end)
        ridership_object[line] = data

    # get data for commuter rail (treated as one line)
    ridership_object["line-commuter-rail"] = combine_commuter_rail_weeks(
        query_landing_ridership_data(constants.commuter_rail_ridership_key(line), start, end)
        for line in constants.COMMUTER_RAIL_LINES
    )

    # Add aggregate bus and ferry ridership
    ridership_object["line-bus"] = query_landing_ridership_data("line-bus", start, end)
    ridership_object["line-ferry"] = query_landing_ridership_data("line-ferry", start, end)

    return ridership_object


def combine_commuter_rail_weeks(weeks_by_line) -> list[dict]:
    """Sum each week's ridership across CR lines into "line-commuter-rail" records, sorted by date.

    Weeks are matched by date, not list position, so a line missing a week (Foxboro is
    irregular) doesn't shift its later weeks into the wrong slots, and any number of weeks fits.
    """
    combined: dict[str, dict] = {}
    for weeks in weeks_by_line:
        for week in weeks:
            entry = combined.setdefault(
                week["date"],
                {"lineId": "line-commuter-rail", "count": 0, "timestamp": week.get("timestamp"), "date": week["date"]},
            )
            entry["count"] += week["count"]
    return [combined[d] for d in sorted(combined)]


def query_landing_ridership_data(line: str, start: str, end: str):
    """Queries ridership data for a line between two dates.

    Args:
        line: The ridership line key (e.g. "line-Red").
        start: First date, YYYY-MM-DD.
        end: Last date, YYYY-MM-DD.

    Returns:
        A list of ridership records for the landing page date range.
    """
    table = dynamodb.Table("Ridership")
    response = table.query(KeyConditionExpression=Key("lineId").eq(line) & Key("date").between(start, end))
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
    print("Uploading ridership and trip metric data for landing page from {} to {}".format(*landing_date_range()))
    trip_metrics_data = get_trip_metrics_data()
    ridership_data = get_ridership_data()
    upload_to_s3(json.dumps(trip_metrics_data), json.dumps(ridership_data))
    clear_cache()
