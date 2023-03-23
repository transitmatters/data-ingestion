from datetime import timedelta, datetime
from decimal import Decimal
import json
from urllib.parse import urlencode
from chalicelib import dynamo, constants
import requests


''' Function to remove traversal time entries which do not have data for each leg of the trip.'''
def remove_invalid_entries(item, expected_entries, date):
    if item["entries"] < expected_entries:
        print(f"Removing invalid entry for ({date}): Insufficient data - 1 or more leg of trip has no data.")
        return False
    return True


def get_agg_tt_api_requests(stops, current_date, delta):
    api_requests = []
    for stop_pair in stops:
        params = {
            "from_stop": stop_pair[0],
            "to_stop": stop_pair[1],
            "start_date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(current_date + delta - timedelta(days=1), constants.DATE_FORMAT_BACKEND),
        }
        url = constants.DD_URL_AGG_TT.format(parameters=urlencode(params))
        api_requests.append(url)
    return api_requests


def send_requests(api_requests):
    tt_object = {}
    for request in api_requests:
        response = requests.get(request)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print(response.content.decode("utf-8"))
            raise
        data = json.loads(response.content.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
        for item in data:
            if item["service_date"] in tt_object:
                tt_object[item['service_date']]["median"] += item['50%']
                tt_object[item['service_date']]["count"] += item['count']
                tt_object[item['service_date']]["entries"] += 1
            else:
                tt_object[item["service_date"]] = {
                    "median": item['50%'] if item['50%'] else 0,
                    "count": item['count'] if item['count'] else 0,
                    "entries": 1,
                }
    return tt_object


def remove_invalid_tt_objects(tt_object, expected_num_entries):
    return filter(lambda item: remove_invalid_entries(item[1], expected_num_entries, item[0]), list(tt_object.items()))

def format_tt_objects(tt_objects, line):
    formatted_tt_objects = []
    for (curr_date, metrics) in tt_objects:
            formatted_tt_objects.append({
                "line": line,
                "date": curr_date,
                "value": metrics["median"],
                "count": metrics["count"]
            })
    return formatted_tt_objects


''' Only should be run manually. Calculates median TTs and trip counts for all days between start and end dates.'''
def populate_daily_table(start_date, end_date, line):
    print(f"populating DailySpeeds for line: {line}")
    stops = constants.TERMINI[line]
    current_date = start_date
    delta = timedelta(days=300)
    tt_objects = []
    while current_date < end_date:
        print(f"Calculating Daily values for 300 day chunk starting at: {current_date}")
        API_requests = get_agg_tt_api_requests(stops, current_date, delta)
        tt_object = send_requests(API_requests)
        # Remove entries which don't have values for all routes.
        tt_object_filtered = remove_invalid_tt_objects(tt_object, len(API_requests))
        tt_object_formatted = format_tt_objects(tt_object_filtered, line)
        tt_objects.extend(tt_object_formatted)
        current_date += delta
    print("Writing objects to DailySpeed table")
    dynamo.write_to_traversal_table(tt_objects, "DailySpeed") 
    print("Done")

def update_daily_table(date):
    tt_objects = []
    for line in constants.LINES:
        stops = constants.TERMINI[line]
        delta = timedelta(days=1)
        date_string = datetime.strftime(date, constants.DATE_FORMAT_BACKEND)
        print(f"Calculating update on [{line}] for date: {date_string}")
        API_requests = get_agg_tt_api_requests(stops, date, delta)
        tt_object = send_requests(API_requests)
        # Remove entries which don't have values for all routes.
        tt_object_filtered = list(remove_invalid_tt_objects(tt_object, len(API_requests)))
        if len(tt_object_filtered) == 0:
            print("No data for date {date_string}")
            return
        tt_objects.extend(format_tt_objects(tt_object_filtered, line))
    print(f"Writing values: {tt_objects}")
    dynamo.write_to_traversal_table(tt_objects, "DailySpeed")
    print("Complete.")
