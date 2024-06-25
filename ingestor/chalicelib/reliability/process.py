from datetime import date, timedelta
from decimal import Decimal
import json
import re
from typing import List
from urllib.parse import urlencode

import pandas as pd
import requests

from ingestor.chalicelib import constants, dynamo
from ingestor.chalicelib.reliability.aggregate import group_weekly_data
from ingestor.chalicelib.reliability.types import Alert, AlertsRequest

TABLE_NAME = "AlertDelaysWeekly"


def generate_requests(start_date: date, end_date: date) -> List[AlertsRequest]:
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
    if (
        "disabled train" in alert["text"].lower()
        or "disabled trolley" in alert["text"].lower()
        or "train that was disabled" in alert["text"].lower()
    ):
        return "disabled_train"
    elif "signal problem" in alert["text"].lower() or "signal issue" in alert["text"].lower():
        return "signal_problem"
    elif "switch problem" in alert["text"].lower():
        return "switch_problem"
    elif "brake issue" in alert["text"].lower() or "brake problem" in alert["text"].lower():
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
        or "wire problem" in alert["text"].lower()
        or "electrical problem" in alert["text"].lower()
        or "overhead catenary" in alert["text"].lower()
        or "third rail wiring" in alert["text"].lower()
    ):
        return "power_problem"
    elif "door problem" in alert["text"].lower() or "door issue" in alert["text"].lower():
        return "door_problem"
    elif (
        "track issue" in alert["text"].lower()
        or "track problem" in alert["text"].lower()
        or "cracked rail" in alert["text"].lower()
    ):
        return "track_issue"
    elif (
        "medical emergency" in alert["text"].lower()
        or "ill passenger" in alert["text"].lower()
        or "medical assistance" in alert["text"].lower()
    ):
        return "medical_emergency"
    elif "flooding" in alert["text"].lower():
        return "flooding"
    elif "police" in alert["text"].lower():
        return "police_activity"
    elif "fire" in alert["text"].lower() or "smoke" in alert["text"].lower() or "burning" in alert["text"].lower():
        return "fire"

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
        "power_problem": 0,
        "door_problem": 0,
        "brake_problem": 0,
        "switch_problem": 0,
        "track_issue": 0,
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


def process_requests(requests: List[AlertsRequest]):
    # process all requests
    all_data = {
        "line-red": [],
        "line-orange": [],
        "line-blue": [],
        "line-green": [],
    }
    for request in requests:
        data = process_single_day(request)
        if data is not None and len(data) != 0:
            total_delay, delay_by_type = process_delay_time(data)
            if total_delay == 0:
                continue
            all_data[constants.ROUTE_TO_LINE_MAP[request.route]].append(
                {
                    "date": request.date.isoformat(),
                    "line": constants.ROUTE_TO_LINE_MAP[request.route],
                    "total_delay_time": total_delay,
                    "delay_by_type": delay_by_type,
                }
            )

    df_data = {}
    for line in constants.LINES:
        # convert to DataFrame
        df = pd.DataFrame(all_data[line])
        df = df.join(pd.json_normalize(df["delay_by_type"]))
        df.drop(columns=["delay_by_type"], inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df_data[line] = df
    return df_data


def update_table(start_date: date, end_date: date):
    alert_requests = generate_requests(start_date, end_date)
    all_data = process_requests(alert_requests)

    grouped_data = []
    for line, df in all_data.items():
        grouped_data.extend(group_weekly_data(df, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(grouped_data), parse_float=Decimal), TABLE_NAME)


if __name__ == "__main__":
    start_date = date(2023, 1, 1)
    end_date = date(2023, 9, 1)
    update_table(start_date, end_date)
