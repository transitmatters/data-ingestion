from datetime import date, timedelta
from decimal import Decimal
import json
import re
from typing import List
from urllib.parse import urlencode

import pandas as pd
import requests

from chalicelib import constants, dynamo
from chalicelib.delays.aggregate import group_weekly_data
from chalicelib.delays.types import Alert, AlertsRequest

TABLE_NAME = "AlertDelaysWeekly"


def generate_requests(start_date: date, end_date: date, lines=constants.ALL_LINES) -> List[AlertsRequest]:
    reqs = []
    date_ranges = []
    current_date = start_date
    while current_date <= end_date:
        date_ranges.append(current_date)
        current_date += timedelta(days=1)
    for current_date in date_ranges:
        for line in lines:
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
    return "delays" in alert["text"].lower() and "minutes" in alert["text"].lower()


def alert_type(alert: Alert):
    if (
        "disabled train" in alert["text"].lower()
        or "disabled trolley" in alert["text"].lower()
        or "train that was disabled" in alert["text"].lower()
        or "disabled bus" in alert["text"].lower()
        or "train being taken out of service" in alert["text"].lower()
        or "train being removed from service" in alert["text"].lower()
    ):
        return "disabled_vehicle"
    elif (
        "signal problem" in alert["text"].lower()
        or "signal issue" in alert["text"].lower()
        or "signal repairs" in alert["text"].lower()
        or "signal maintenance" in alert["text"].lower()
        or "signal repair" in alert["text"].lower()
        or "signal work" in alert["text"].lower()
        or "signal department" in alert["text"].lower()
    ):
        return "signal_problem"
    elif (
        "switch problem" in alert["text"].lower()
        or "switch issue" in alert["text"].lower()
        or "witch problem" in alert["text"].lower()
        or "switching issue" in alert["text"].lower()
    ):
        return "switch_problem"
    elif (
        "brake issue" in alert["text"].lower()
        or "brake problem" in alert["text"].lower()
        or "brakes activated" in alert["text"].lower()
        or "brakes holding" in alert["text"].lower()
        or "brakes applied" in alert["text"].lower()
    ):
        return "brake_problem"
    elif (
        "power problem" in alert["text"].lower()
        or "power issue" in alert["text"].lower()
        or "overhead wires" in alert["text"].lower()
        or "overhead wire" in alert["text"].lower()
        or "overhear wires" in alert["text"].lower()  # typo in the alert
        or "overheard wires" in alert["text"].lower()  # typo in the alert
        or "catenary wires" in alert["text"].lower()
        or "the overhead" in alert["text"].lower()
        or "wire repair" in alert["text"].lower()
        or "repairs to the wire" in alert["text"].lower()
        or "wire maintenance" in alert["text"].lower()
        or "wire inspection" in alert["text"].lower()
        or "wire problem" in alert["text"].lower()
        or "electrical problem" in alert["text"].lower()
        or "overhead catenary" in alert["text"].lower()
        or "third rail wiring" in alert["text"].lower()
        or "power department work" in alert["text"].lower()
    ):
        return "power_problem"
    elif "door problem" in alert["text"].lower() or "door issue" in alert["text"].lower():
        return "door_problem"
    elif (
        "track issue" in alert["text"].lower()
        or "track problem" in alert["text"].lower()
        or "cracked rail" in alert["text"].lower()
        or "broken rail" in alert["text"].lower()
    ):
        return "track_issue"
    elif (
        "medical emergency" in alert["text"].lower()
        or "ill passenger" in alert["text"].lower()
        or "medical assistance" in alert["text"].lower()
        or "medical attention" in alert["text"].lower()
        or "sick passenger" in alert["text"].lower()
    ):
        return "medical_emergency"
    elif "flooding" in alert["text"].lower():
        return "flooding"
    elif "police" in alert["text"].lower():
        return "police_activity"
    elif "fire" in alert["text"].lower() or "smoke" in alert["text"].lower() or "burning" in alert["text"].lower():
        return "fire"
    elif (
        "mechanical problem" in alert["text"].lower()
        or "mechanical issue" in alert["text"].lower()
        or "motor problem" in alert["text"].lower()
        or "pantograph problem" in alert["text"].lower()
        or "pantograph issue" in alert["text"].lower()
        or "issue with the heating system" in alert["text"].lower()
        or "air pressure problem" in alert["text"].lower()
    ):
        return "mechanical_problem"
    elif (
        "track work" in alert["text"].lower()
        or "track maintenance" in alert["text"].lower()
        or "overnight work" in alert["text"].lower()
        or "track repair" in alert["text"].lower()
        or "personnel performed maintenance" in alert["text"].lower()
        or "maintenance work" in alert["text"].lower()
        or "overnight maintenance" in alert["text"].lower()
    ):
        return "track_work"
    elif (
        "unauthorized vehicle on the tracks" in alert["text"].lower()
        or "vehicle blocking the tracks" in alert["text"].lower()
        or "auto accident" in alert["text"].lower()
        or "car on the tracks" in alert["text"].lower()
        or "car blocking the tracks" in alert["text"].lower()
        or "car accident" in alert["text"].lower()
        or "automobile accident" in alert["text"].lower()
        or "car blocking the tracks" in alert["text"].lower()
        or "disabled vehicle on the tracks" in alert["text"].lower()
        or "due to traffic" in alert["text"].lower()
        or "car in the track area" in alert["text"].lower()
        or "car blocking the track area" in alert["text"].lower()
        or "auto that was blocking" in alert["text"].lower()
        or "auto blocking the track" in alert["text"].lower()
        or "auto was removed from the track" in alert["text"].lower()
    ):
        return "car_traffic"

    print(alert["valid_from"], alert["text"].lower())
    return "other"


def process_delay_time(alerts: List[Alert]):
    delays = []
    for alert in alerts:
        if not alert_is_delay(alert):
            continue
        delay_time = re.findall(r"delays of about \d+ minutes", alert["text"].lower())
        if (delay_time is None) or (len(delay_time) == 0):
            # try another pattern, since the first one didn't match
            # less accurate, but better than nothing
            delay_time = re.findall(r"delays of up to \d+ minutes", alert["text"].lower())
        if (delay_time is not None) and (len(delay_time) != 0):
            delays.append(
                {
                    "delay_time": delay_time[0],
                    "alert_type": alert_type(alert),
                }
            )
    total_delay = 0
    delay_by_type = {
        "disabled_vehicle": 0,
        "signal_problem": 0,
        "power_problem": 0,
        "door_problem": 0,
        "brake_problem": 0,
        "switch_problem": 0,
        "track_issue": 0,
        "mechanical_problem": 0,
        "track_work": 0,
        "car_traffic": 0,
        "police_activity": 0,
        "medical_emergency": 0,
        "fire": 0,
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


def process_requests(requests: List[AlertsRequest], lines=constants.ALL_LINES):
    # process all requests
    all_data = {}
    for line in lines:
        all_data[line] = []

    for request in requests:
        print(request.route, request.date)
        data = process_single_day(request)
        if data is not None and len(data) != 0:
            total_delay, delay_by_type = process_delay_time(data)
            all_data[request.route].append(
                {
                    "date": request.date.isoformat(),
                    "line": request.route,
                    "total_delay_time": total_delay,
                    "delay_by_type": delay_by_type,
                }
            )

    df_data = {}
    for line in lines:
        df = pd.DataFrame(all_data[line])
        df = df.join(pd.json_normalize(df["delay_by_type"]))
        df.drop(columns=["delay_by_type"], inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df_data[line] = df
    return df_data


def update_table(start_date: date, end_date: date, lines=constants.ALL_LINES):
    """
    Update the table with rapid transit data
    """
    alert_requests = generate_requests(start_date, end_date, lines)
    all_data = process_requests(alert_requests, lines)

    grouped_data = []
    for line, df in all_data.items():
        grouped_data.extend(group_weekly_data(df, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(grouped_data), parse_float=Decimal), TABLE_NAME)


if __name__ == "__main__":
    start_date = date(2024, 6, 1)
    end_date = date(2024, 12, 20)
    update_table(start_date, end_date, constants.ALL_LINES)
