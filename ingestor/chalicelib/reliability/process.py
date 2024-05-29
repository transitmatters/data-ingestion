from datetime import date, timedelta
import json
import re
from typing import List
from urllib.parse import urlencode

import requests

from ingestor.chalicelib import constants
from ingestor.chalicelib.reliability.types import Alert, AlertsRequest


def generate_requests(
    start_date: date,
    end_date: date,
) -> List[AlertsRequest]:
    reqs = []
    date_ranges = []
    current_date = start_date
    while current_date <= end_date:
        date_ranges.append(current_date)
        current_date += timedelta(days=1)
    for current_date in date_ranges:
        for line in constants.ALL_LINES:
            request = AlertsRequest(
                route=line,
                date=current_date,
            )
            reqs.append(request)
    return reqs


def process_single_day(request: AlertsRequest):
    params = {
        "route": request.route,
    }
    # process a single day of alerts
    request_url = constants.DD_URL_ALERTS.format(
        date=request.date.strftime(constants.DATE_FORMAT_BACKEND), parameters=urlencode(params, doseq=True)
    )
    response = requests.get(request_url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(response.content.decode("utf-8"))
        raise
    return json.loads(response.content.decode("utf-8"))


def alert_is_delay(alert: Alert):
    return "delays of about" in alert["text"].lower()


def alert_type(alert: Alert):
    if "disabled train" in alert["text"].lower():
        return "disabled_train"
    elif "signal problem" in alert["text"].lower() or "signal issue" in alert["text"].lower():
        return "signal_problem"
    elif "door problem" in alert["text"].lower():
        return "door_problem"
    elif "medical emergency" in alert["text"].lower():
        return "medical_emergency"
    elif "flooding" in alert["text"].lower():
        return "flooding"
    elif "police" in alert["text"].lower():
        return "police_activity"

    print(alert["text"].lower())
    return "other"


def process_delay_time(alerts: List[Alert]):
    delays = []
    for alert in alerts:
        if not alert_is_delay(alert):
            continue
        delay_time = re.findall(r"delays of about \d+ minutes", alert["text"].lower())
        if (delay_time is not None) and (len(delay_time) != 0):
            delays.append(
                {
                    "delay_time": delay_time[0],
                    "alert_type": alert_type(alert),
                }
            )
    total_delay = 0
    delay_by_type = {
        "disabled_train": 0,
        "signal_problem": 0,
        "door_problem": 0,
        "police_activity": 0,
        "medical_emergency": 0,
        "flooding": 0,
        "other": 0,
    }
    for delay in delays:
        if (delay is None) or (len(delay) == 0):
            continue
        res = list(map(int, re.findall(r"\d+", delay["delay_time"])))
        total_delay += res[0]
        delay_by_type[delay["alert_type"]] += res[0]
    return total_delay, delay_by_type


def process_requests(requests: List[AlertsRequest]):
    # process all requests
    all_data = {"Red": [], "Orange": [], "Blue": [], "Green-B": [], "Green-C": [], "Green-D": [], "Green-E": []}
    for request in requests:
        data = process_single_day(request)
        if data is not None and len(data) != 0:
            total_delay, delay_by_type = process_delay_time(data)
            if total_delay == 0:
                continue
            all_data[request.route].append(
                {
                    "date": request.date.strftime(constants.DATE_FORMAT_BACKEND),
                    "delay_time": total_delay,
                    "delay_by_type": delay_by_type,
                }
            )
    return all_data


if __name__ == "__main__":
    start_date = date(2024, 1, 10)
    end_date = date(2024, 1, 25)
    alert_requests = generate_requests(start_date, end_date)
    all_data = process_requests(alert_requests)
    # Save result to JSON file
    with open("output.json", "w") as f:
        json.dump(all_data, f)
