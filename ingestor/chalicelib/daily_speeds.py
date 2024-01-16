from datetime import date, timedelta, datetime
from decimal import Decimal
import json
from urllib.parse import urlencode
from chalicelib import dynamo, constants
import requests


def is_valid_entry(item, expected_entries, date):
    """Function to remove traversal time entries which do not have data for each leg of the trip."""
    if item["entries"] < expected_entries:
        print(f"No speed value for ({date}): Insufficient data.")
        return False
    return True


def get_agg_tt_api_requests(stops, current_date, delta):
    """Create API requests from parameters"""
    api_requests = []
    for stop_pair in stops:
        params = {
            "from_stop": stop_pair[0],
            "to_stop": stop_pair[1],
            "start_date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(current_date + delta - timedelta(days=1), constants.DATE_FORMAT_BACKEND),
        }
        url = constants.DD_URL_AGG_TT.format(parameters=urlencode(params, doseq=True))
        api_requests.append(url)
    return api_requests


def send_requests(api_requests):
    """Send API requests to Datadashboard backend."""
    speed_object = {}
    for request in api_requests:
        response = requests.get(request)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print(response.content.decode("utf-8"))
            raise
        data = json.loads(response.content.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
        for item in data:
            if item["service_date"] in speed_object:
                speed_object[item["service_date"]]["median"] += item["50%"]
                speed_object[item["service_date"]]["mean"] += item["mean"]
                speed_object[item["service_date"]]["count"] += item["count"] / 2
                speed_object[item["service_date"]]["entries"] += 1
            else:
                speed_object[item["service_date"]] = {
                    "median": item["50%"] if item["50%"] else 0,
                    "count": item["count"] / 2 if item["count"] else 0,
                    "mean": item["mean"] if item["mean"] else 0,
                    "entries": 1,
                }
    return speed_object


def format_tt_objects(
    speed_objects, route_metadata, line: str, route: str | None, expected_num_entries, date_range: list[str]
):
    """Remove invalid entries and format for Dynamo."""
    formatted_speed_objects = []
    route_name = f"{line}-{route}"

    for current_date in date_range:
        metrics = speed_objects.get(current_date)
        new_speed_object = {
            "route": route_name,
            "line": line,
            "date": current_date,
            "count": None,
        }
        if metrics:
            new_speed_object["count"] = metrics["count"]
        if metrics and is_valid_entry(metrics, expected_num_entries, current_date):
            new_speed_object["median"] = metrics["median"]
            new_speed_object["mean"] = round(metrics["mean"], 1)
            new_speed_object["miles_covered"] = metrics["count"] * Decimal(route_metadata["length"])
            new_speed_object["track_mileage"] = Decimal(route_metadata["length"])
            new_speed_object["total_time"] = round(metrics["mean"], 1) * metrics["count"]

        formatted_speed_objects.append(new_speed_object)
    return formatted_speed_objects


def get_date_range_strings(start_date: date, end_date: date):
    date_range: list[str] = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return date_range


def populate_daily_table(start_date: datetime, end_date: datetime, line: str, route: str | None):
    """Populate DeliveredTripMetrics table. Calculates median TTs and trip counts for all days between start and end dates."""
    print(f"populating DeliveredTripMetrics for Line/Route: {line}/{route if route else '(no-route)'}")
    current_date = start_date.date()
    delta = timedelta(days=180)
    speed_objects = []
    while current_date < end_date:
        route_metadata = constants.get_route_metadata(line, current_date, False, route)
        print(f"Calculating daily values for 180 day chunk starting at: {current_date}")
        API_requests = get_agg_tt_api_requests(route_metadata["stops"], current_date, delta)
        curr_speed_object = send_requests(API_requests)
        date_range = get_date_range_strings(current_date, current_date + delta - timedelta(days=1))
        formatted_speed_object = format_tt_objects(
            curr_speed_object, route_metadata, line, route, len(API_requests), date_range
        )
        speed_objects.extend(formatted_speed_object)
        if (
            line == "line-green"
            and current_date < constants.GLX_EXTENSION_DATE
            and current_date + delta >= constants.GLX_EXTENSION_DATE
        ):
            current_date = constants.GLX_EXTENSION_DATE
        else:
            current_date += delta
        dynamo.dynamo_batch_write(speed_objects, "DeliveredTripMetrics")
        speed_objects = []
        print("Writing objects to DeliveredTripMetrics table")
        print("Done")


def update_daily_table(date):
    """Update DailySpeed table"""
    speed_objects = []
    for route in constants.ALL_ROUTES:
        line = route[0]
        route = route[1]
        route_metadata = constants.get_route_metadata(line, date, False, route)
        delta = timedelta(days=1)
        date_string = datetime.strftime(date, constants.DATE_FORMAT_BACKEND)
        print(f"Calculating update on [{line}/{route if route else '(no-route)'}] for date: {date_string}")
        API_requests = get_agg_tt_api_requests(route_metadata["stops"], date, delta)
        speed_object = send_requests(API_requests)
        formatted_speed_object = format_tt_objects(
            speed_object,
            route_metadata,
            line,
            route,
            len(API_requests),
            [date_string],
        )
        if len(formatted_speed_object) == 0:
            print("No data for date {date_string}")
            continue
        speed_objects.extend(formatted_speed_object)
    print(f"Writing values: {speed_objects}")
    dynamo.dynamo_batch_write(speed_objects, "DeliveredTripMetrics")
    print("Complete.")
