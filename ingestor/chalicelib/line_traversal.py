from datetime import timedelta, datetime
from decimal import Decimal
import json
from urllib.parse import urlencode
from chalicelib import dynamo, constants
import requests


''' Function to remove traversal time entries which do not have data for each leg of the trip.'''
def remove_invalid_entries(item, expected_entries, date):
    if item["entries"] < expected_entries:
        print('removing:', date, item["entries"])
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


def get_tt_api_requests(stops, current_date):
    api_requests = []
    for stop_pair in stops:
        params = {
            "from_stop": stop_pair[0],
            "to_stop": stop_pair[1],
        }
        url = constants.DD_URL_SINGLE_TT.format(date=datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND), parameters=urlencode(params))
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


''' Only should be run manually. Calculates median TTs and trip counts for all days between start and end dates.'''
def populate_daily_table(start_date, end_date, line):
    stops = constants.TERMINI[line]
    current_date = start_date
    delta = timedelta(days=24)
    while current_date < end_date:
        API_requests = get_agg_tt_api_requests(stops, current_date, delta)
        tt_object = send_requests(API_requests)
        # Remove entries which don't have values for all routes.
        tt_object_filtered = remove_invalid_tt_objects(tt_object, len(API_requests))
        dynamo.write_to_traversal_table(list(tt_object_filtered), line, "LineTraversalTime")
        print(current_date)
        current_date += delta


def update_daily_table(date):
    for line in constants.LINES:
        stops = constants.TERMINI[line]
        delta = timedelta(days=1)
        date_string = datetime.strftime(date, constants.DATE_FORMAT_BACKEND)
        API_requests = get_agg_tt_api_requests(stops, date, delta)
        tt_object = send_requests(API_requests)
        # Remove entries which don't have values for all routes.
        tt_object_filtered = list(remove_invalid_tt_objects(tt_object, len(API_requests)))
        if len(tt_object_filtered) == 0:
            print("No data for date {date_string}")
            return
        print(f"Updating [{line}] for date: {date_string}")
        dynamo.write_to_traversal_table(tt_object_filtered, line, "LineTraversalTime")
