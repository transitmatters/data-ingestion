from decimal import Decimal
import json
from urllib.parse import urlencode
from datetime import datetime
import requests
from chalicelib import dynamo, constants

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


def update_scheduled_speed_entry(date):
    scheduled_speed_objects = []
    for line in constants.LINES:
        benchmark = 0
        api_requests = get_tt_api_requests(constants.TERMINI[line], date)
        for request in api_requests:
            response = requests.get(request)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError:
                print(response.content.decode("utf-8"))
                raise
            data = json.loads(response.content.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
            if len(data) == 0:
                print("No data")
                return
            benchmark += sum(tt["benchmark_travel_time_sec"] for tt in data if "benchmark_travel_time_sec" in tt) / len(data)
        scheduled_speed_objects.append({
            "line": line,
            "date": datetime.strftime(date, constants.DATE_FORMAT_BACKEND),
            "value": Decimal(benchmark)
        })
    dynamo.dynamo_batch_write(scheduled_speed_objects)