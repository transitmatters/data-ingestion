import datetime
from decimal import Decimal
import json
from urllib.parse import urlencode
import boto3
from datetime import datetime
import requests

from chalicelib import dynamo, constants


dyn_resource = boto3.resource("dynamodb")
table = dyn_resource.Table("OverviewStats")




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



# TODO: Maybe this should be a x hour rolling average of the past x hours of active service (?) or maybe last x trips
def update_current_schedule_adherence():
    for line in constants.LINES:
        total_time = 0
        benchmark = 0
        now = datetime.datetime.now()
        today = now
        if now.hour < 6:  # get yesterday's speeds until 6 am.
            today = today.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1) + datetime.timedelta(hours=23, minutes=59)
        api_requests = get_tt_api_requests(constants.TERMINI[line], now)
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
            total_time += sum(tt["travel_time_sec"] for tt in data if "travel_time_sec" in tt)
            benchmark += sum(tt["benchmark_travel_time_sec"] for tt in data if "benchmark_travel_time_sec" in tt)
        percentage = int(100 * benchmark / total_time) if total_time > 0 else 0
        dynamo.update_speed_adherence(line, now, percentage)
