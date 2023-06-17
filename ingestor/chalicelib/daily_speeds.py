from datetime import timedelta, datetime
from decimal import Decimal
import json
from urllib.parse import urlencode
from chalicelib import dynamo, constants
import requests



def is_valid_entry(item, expected_entries, date):
    ''' Function to remove traversal time entries which do not have data for each leg of the trip.'''
    if item["entries"] < expected_entries:
        print(f"No speed value for ({date}): Insufficient data. {item['entries']}/{expected_entries}")
        return False
    return True


def get_agg_tt_api_requests(stops, current_date, delta):
    ''' Create API requests from parameters '''
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
    ''' Send API requests to Datadashboard backend. '''
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
                speed_object[item['service_date']]["median"] += item['50%']
                speed_object[item['service_date']]["count"] += item['count']
                speed_object[item['service_date']]["entries"] += 1
            else:
                speed_object[item["service_date"]] = {
                    "median": item['50%'] if item['50%'] else 0,
                    "count": item['count'] if item['count'] else 0,
                    "entries": 1,
                }
    return speed_object

def format_tt_objects(speed_objects, line, expected_num_entries, date_range):
    ''' Remove invalid entries and format for Dynamo. '''
    formatted_speed_objects = []
    for current_date in date_range:
        metrics = speed_objects.get(current_date)
        new_speed_object = {
            "line": line,
            "date": current_date,
            "value": None,
            "count": None,
        }

        if metrics:
            new_speed_object["count"] = metrics["count"]
        if metrics and is_valid_entry(metrics, expected_num_entries, current_date):
            new_speed_object["value"] = metrics["median"]

        formatted_speed_objects.append(new_speed_object)
    return formatted_speed_objects

def get_date_range_strings(start_date, end_date):
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    return date_range

def populate_daily_table(start_date, end_date, line):
    ''' Populate DailySpeed table. Calculates median TTs and trip counts for all days between start and end dates.'''
    print(f"populating DailySpeed for line: {line}")
    current_date = start_date
    delta = timedelta(days=10)
    speed_objects = []
    while current_date < end_date:
        stops = constants.get_stops(line, current_date)
        print(f"Calculating daily values for 300 day chunk starting at: {current_date}")
        API_requests = get_agg_tt_api_requests(stops, current_date, delta)
        print(API_requests)
        curr_speed_object = send_requests(API_requests)
        date_range = get_date_range_strings(current_date, current_date + delta - timedelta(days=1))
        formatted_speed_object = format_tt_objects(curr_speed_object, 'line-green' if line == 'line-green-glx' else line, len(API_requests), date_range)
        speed_objects.extend(formatted_speed_object)
        if(line == 'line-green' and current_date < constants.GLX_EXTENSION and current_date + delta >= constants.GLX_EXTENSION):
            current_date = constants.GLX_EXTENSION
        current_date += delta
    dynamo.dynamo_batch_write(speed_objects, "DailySpeed") 
    print("Writing objects to DailySpeed table")
    print("Done")


def update_daily_table(date):
    ''' Update DailySpeed table'''
    speed_objects = []
    for line in constants.LINES:
        stops = constants.TERMINI[line]
        delta = timedelta(days=1)
        date_string = datetime.strftime(date, constants.DATE_FORMAT_BACKEND)
        print(f"Calculating update on [{line}] for date: {date_string}")
        API_requests = get_agg_tt_api_requests(stops, date, delta)
        tt_object = send_requests(API_requests)
        formatted_speed_objects = format_tt_objects(tt_object, line, len(API_requests), [date_string])
        if len(formatted_speed_objects) == 0:
            print("No data for date {date_string}")
            continue
        speed_objects.extend(formatted_speed_objects)
    print(f"Writing values: {speed_objects}")
    dynamo.dynamo_batch_write(speed_objects, "DailySpeed")
    print("Complete.")
