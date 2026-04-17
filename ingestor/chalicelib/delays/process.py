import json
import re
from datetime import date, timedelta
from decimal import Decimal
from typing import List
from urllib.parse import urlencode

import pandas as pd
import requests
from boto3.dynamodb.conditions import Key

from .. import constants, dynamo
from .aggregate import group_daily_data, group_weekly_data
from .types import Alert, AlertsRequest

WEEKLY_TABLE_NAME = "AlertDelaysWeekly"
DAILY_TABLE_NAME = "AlertDelaysDaily"


def generate_requests(start_date: date, end_date: date, lines=constants.ALL_LINES) -> List[AlertsRequest]:
    reqs = []
    date_ranges = []
    current_date = start_date
    while current_date <= end_date:
        date_ranges.append(current_date)
        current_date += timedelta(days=1)

    for current_date in date_ranges:
        for line in lines:
            if not is_line_active(line, current_date):
                continue
            request = AlertsRequest(
                route=line,
                date=current_date,
            )
            reqs.append(request)
    return reqs


def is_line_active(line: str, check_date: date) -> bool:
    """
    Check if a line was active on a given date
    """
    if line == "CR-Middleborough":
        return check_date < constants.CR_MIDDLEBOROUGH_DISCONTINUED
    elif line == "CR-NewBedford":
        return check_date >= constants.CR_MIDDLEBOROUGH_DISCONTINUED

    # Remains active for lines without cutoff dates
    return True


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
    text = alert["text"].lower()
    return (
        ("delays" in text and "minutes" in text)  # Original subway pattern
        or "minutes late" in text  # New commuter rail patterns
        or "minutes behind schedule" in text
        or "behind schedule" in text
    )


def alert_type(alert: Alert):
    text_lower = alert["text"].lower()

    # Check each alert type pattern
    for alert_type_label, patterns in constants.ALERT_PATTERNS.items():
        for pattern in patterns:
            if pattern in text_lower:
                return alert_type_label

    # print(alert["valid_from"], alert["text"].lower())
    return "other"


def process_delay_time(alerts: List[Alert]):
    delays = []

    patterns = [
        r"delays of about \d+ minutes",  # For subway lines (non-CR)
        r"delays of up to \d+ minutes",
        r"\d+ minutes late",  # These 4 are for Commuter Rail
        r"\d+ minutes behind schedule",
        r"\d+\s*-\s*\d+ minutes behind schedule",  # \s* just in case there is extra spacing in the message
        r"\d+\s*-\s*\d+ minutes late",
    ]

    for alert in alerts:
        if not alert_is_delay(alert):
            continue

        # 1st Case
        for pattern in patterns:
            delay_time = re.findall(pattern, alert["text"].lower())
            if delay_time:
                delays.append(
                    {
                        "delay_time": delay_time[0],
                        "alert_type": alert_type(alert),
                    }
                )
                break

    total_delay = 0
    delay_by_type = constants.DELAY_BY_TYPE.copy()

    for delay in delays:
        if (delay is None) or (len(delay) == 0):
            continue

        res = list(map(int, re.findall(r"\d+", delay["delay_time"])))
        if res:
            delay_minutes = max(res)  # Take highest number for ranges
            total_delay += delay_minutes
            delay_by_type[delay["alert_type"]] += delay_minutes

    return total_delay, delay_by_type


def process_requests(requests: List[AlertsRequest], lines=constants.ALL_LINES):
    # process all requests
    all_data = {}
    for line in lines:
        all_data[line] = []

    for request in requests:
        data = process_single_day(request)
        # Initializing at 0 regardless of condition
        total_delay = 0
        delay_by_type = constants.DELAY_BY_TYPE.copy()

        if data is not None and len(data) != 0:
            total_delay, delay_by_type = process_delay_time(data)
        # We should always append zero records just in case
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

        # FIX: this skips over empty dataframes, which only occur if the line is inactive
        # Note: active lines with no data still get appended with zeroes
        if df.empty:
            continue

        df = df.join(pd.json_normalize(df["delay_by_type"]))
        df.drop(columns=["delay_by_type"], inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df_data[line] = df
    return df_data


def get_daily_data_for_week(start_date: date, end_date: date, lines=constants.ALL_LINES):
    """
    Query daily data from DynamoDB for weekly aggregation.
    Done as opposed to performing another API call for weekly aggregation.
    Also guarantees complete info for every day.
    """
    daily_records = []

    for line in lines:
        # Convert dates to ISO format strings to match DynamoDB storage format
        start_date_str = start_date.isoformat()
        end_date_str = end_date.isoformat()

        # Query DynamoDB for this line and date range
        params = {
            "KeyConditionExpression": Key("line").eq(line)
            & Key("date").between(start_date_str, end_date_str)  # Use string format
        }
        try:
            line_data = dynamo.query_dynamo(params, DAILY_TABLE_NAME)
            daily_records.extend(line_data)
        except Exception as e:
            print(f"Error querying {line}: {e}")

    return daily_records


def aggregate_commuter_rail_df(df_data: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    """Combine all CR-* line DataFrames into a single 'CommuterRail' aggregate."""
    cr_dfs = [df for line, df in df_data.items() if line.startswith("CR-")]
    if not cr_dfs:
        return None
    cr_combined = pd.concat(cr_dfs)
    cr_combined["line"] = "CommuterRail"
    return cr_combined


def update_weekly_from_daily(start_date: date, end_date: date, lines=constants.ALL_LINES):
    """
    Update weekly table by aggregating daily data from DynamoDB.
    Avoids another api call.
    """

    # Get daily data from DynamoDB instead of API
    daily_records = get_daily_data_for_week(start_date, end_date, lines)

    # Convert to DataFrame and process
    df = pd.DataFrame(daily_records)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    # Aggregate weekly for each line
    weekly_data = []
    line_dfs = {}
    for line, line_df in df.groupby("line"):
        weekly_data.extend(group_weekly_data(line_df, start_date.isoformat()))
        line_dfs[line] = line_df

    # Aggregate commuter rail into a single "CommuterRail" entry
    cr_combined = aggregate_commuter_rail_df(line_dfs)
    if cr_combined is not None:
        weekly_data.extend(group_weekly_data(cr_combined, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(weekly_data, default=int), parse_float=Decimal), WEEKLY_TABLE_NAME)


def update_table(start_date: date, end_date: date, lines=constants.ALL_LINES):
    """
    Update the table with rapid transit data
    """
    alert_requests = generate_requests(start_date, end_date, lines)
    all_data = process_requests(alert_requests, lines)

    grouped_data = []
    for line, df in all_data.items():
        grouped_data.extend(group_daily_data(df, start_date.isoformat()))

    # Aggregate commuter rail into a single "CommuterRail" entry
    cr_combined = aggregate_commuter_rail_df(all_data)
    if cr_combined is not None:
        grouped_data.extend(group_daily_data(cr_combined, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(grouped_data), parse_float=Decimal), DAILY_TABLE_NAME)


# Testing daily updates. Using random dates. Feel free to change and uncomment as needed.
if __name__ == "__main__":
    start_date = date(2025, 12, 1)
    end_date = date(2026, 2, 13)
    update_table(start_date, end_date, constants.ALL_LINES)
    update_weekly_from_daily(start_date, end_date, constants.ALL_LINES)
