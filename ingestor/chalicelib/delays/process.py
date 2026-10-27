import json
import re
import sys
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Optional
from urllib.parse import urlencode

import pandas as pd
import requests
from boto3.dynamodb.conditions import Key

from .. import constants, dynamo
from . import duration
from .aggregate import group_daily_data, group_weekly_data
from .bus import BUS_AGGREGATE, bus_alerts_by_route
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


def alert_type(alert: Alert, bus: bool = False):
    text_lower = alert["text"].lower()

    # Check each alert type pattern. Bus-only patterns go last so they never beat a
    # more specific cause.
    pattern_sets = [constants.ALERT_PATTERNS, constants.BUS_ALERT_PATTERNS] if bus else [constants.ALERT_PATTERNS]
    for pattern_set in pattern_sets:
        for alert_type_label, patterns in pattern_set.items():
            for pattern in patterns:
                if pattern in text_lower:
                    return alert_type_label

    # print(alert["valid_from"], alert["text"].lower())
    return "other"


def process_delay_time(alerts: List[Alert], bus: bool = False):
    delays = []

    patterns = [
        r"delays of about \d+ minutes",  # For subway lines (non-CR) and bus
        r"delays of up to \d+ minutes",
        r"delays of more than \d+ minutes",  # Counted as the stated minimum
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
                        "alert_type": alert_type(alert, bus),
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


def _lamp_alerts(day: date, lamp_alerts_by_day: Optional[dict]):
    """The LAMP day file, through `lamp_alerts_by_day` when given so subway and bus share one download."""
    if lamp_alerts_by_day is None:
        return duration.load_lamp_alerts(day)
    if day not in lamp_alerts_by_day:
        lamp_alerts_by_day[day] = duration.load_lamp_alerts(day)
    return lamp_alerts_by_day[day]


def _add_live_minutes(record: dict, lamp_alerts: dict, line: str, day: date, bus: bool = False):
    """How long the same alerts were actually live, next to what they said."""
    intervals = duration.live_intervals(lamp_alerts, line, day, alert_is_delay)
    record["delay_alert_minutes"] = duration.union_minutes(intervals)
    record["delay_alert_minutes_by_type"] = {
        delay_type: duration.union_minutes([i for i in intervals if alert_type({"text": i[0]}, bus) == delay_type])
        for delay_type in constants.DELAY_BY_TYPE
    }


def process_requests(requests: List[AlertsRequest], lines=constants.ALL_LINES, lamp_alerts_by_day=None):
    # process all requests
    all_data = {}
    for line in lines:
        all_data[line] = []
    if lamp_alerts_by_day is None:
        lamp_alerts_by_day = {}

    for request in requests:
        data = process_single_day(request)
        # Initializing at 0 regardless of condition
        total_delay = 0
        delay_by_type = constants.DELAY_BY_TYPE.copy()

        if data is not None and len(data) != 0:
            total_delay, delay_by_type = process_delay_time(data)
        # We should always append zero records just in case
        record = {
            "date": request.date.isoformat(),
            "line": request.route,
            "total_delay_time": total_delay,
            "delay_by_type": delay_by_type,
        }

        # How long the same alerts were actually live, next to what they said. Commuter
        # Rail's stated "N minutes behind schedule" already is the delay (see duration.py).
        lamp_alerts = None
        if not request.route.startswith("CR-"):
            lamp_alerts = _lamp_alerts(request.date, lamp_alerts_by_day)
        if lamp_alerts is not None:
            _add_live_minutes(record, lamp_alerts, request.route, request.date)
        all_data[request.route].append(record)

    return _to_dataframes(all_data)


def _bus_day_records(day: date, lamp_alerts: dict) -> list[dict]:
    """One record per bus route that had delay alerts on `day`, plus the "Bus" total.

    Routes without delays get no record (there are ~150 of them), but the total is
    always written so a quiet day reads as zero rather than missing.
    """
    total = {
        "date": day.isoformat(),
        "line": BUS_AGGREGATE,
        "total_delay_time": 0,
        "delay_by_type": constants.DELAY_BY_TYPE.copy(),
        "delay_alert_minutes": 0,
        "delay_alert_minutes_by_type": constants.DELAY_BY_TYPE.copy(),
    }
    records = []
    for route, alerts in bus_alerts_by_route(lamp_alerts).items():
        total_delay, delay_by_type = process_delay_time(alerts, bus=True)
        if total_delay == 0:
            continue
        record = {
            "date": day.isoformat(),
            "line": route,
            "total_delay_time": total_delay,
            "delay_by_type": delay_by_type,
        }
        _add_live_minutes(record, lamp_alerts, route, day, bus=True)
        records.append(record)

        # Summed per route, so an alert on two routes counts twice, like the CommuterRail total
        total["total_delay_time"] += total_delay
        total["delay_alert_minutes"] += record["delay_alert_minutes"]
        for delay_type in constants.DELAY_BY_TYPE:
            total["delay_by_type"][delay_type] += delay_by_type[delay_type]
            total["delay_alert_minutes_by_type"][delay_type] += record["delay_alert_minutes_by_type"][delay_type]
    return [*records, total]


def process_bus_days(start_date: date, end_date: date, lamp_alerts_by_day=None) -> dict[str, pd.DataFrame]:
    """Bus delays per route, and for all of bus, for each day with a LAMP alerts file.

    Days without one (today, or before 2019-09-15) are skipped. Without a
    `lamp_alerts_by_day` cache each day's file is dropped once processed.
    """
    all_data: dict[str, list[dict]] = {}
    current_date = start_date
    while current_date <= end_date:
        lamp_alerts = _lamp_alerts(current_date, lamp_alerts_by_day)
        if lamp_alerts is not None:
            for record in _bus_day_records(current_date, lamp_alerts):
                all_data.setdefault(record["line"], []).append(record)
        current_date += timedelta(days=1)
    return _to_dataframes(all_data)


def _to_dataframes(all_data: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    df_data = {}
    for line, records in all_data.items():
        df = pd.DataFrame(records)

        # FIX: this skips over empty dataframes, which only occur if the line is inactive
        # Note: active lines with no data still get appended with zeroes
        if df.empty:
            continue

        df = df.join(pd.json_normalize(df["delay_by_type"]))
        df.drop(columns=["delay_by_type"], inplace=True)
        if "delay_alert_minutes_by_type" in df.columns:
            by_type = df["delay_alert_minutes_by_type"].apply(lambda v: v if isinstance(v, dict) else {})
            df = df.join(pd.json_normalize(by_type).add_prefix("delay_alert_minutes_"))
            df.drop(columns=["delay_alert_minutes_by_type"], inplace=True)
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


def update_weekly_from_daily(start_date: date, end_date: date, lines=constants.ALL_LINES, include_bus=True):
    """
    Update weekly table by aggregating daily data from DynamoDB.
    Avoids another api call.

    Bus routes have no fixed list to query by, so bus is re-read from the LAMP files instead.
    """

    # Snap to Monday so the first week isn't partial. Otherwise a Sunday start overwrites
    # that whole week with just Sunday's delays.
    start_date = start_date - timedelta(days=start_date.weekday())

    # Get daily data from DynamoDB instead of API
    daily_records = get_daily_data_for_week(start_date, end_date, lines)

    # Aggregate weekly for each line
    weekly_data = []
    line_dfs = {}
    if daily_records:
        df = pd.DataFrame(daily_records)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        for line, line_df in df.groupby("line"):
            weekly_data.extend(group_weekly_data(line_df, start_date.isoformat()))
            line_dfs[line] = line_df

    # Aggregate commuter rail into a single "CommuterRail" entry
    cr_combined = aggregate_commuter_rail_df(line_dfs)
    if cr_combined is not None:
        weekly_data.extend(group_weekly_data(cr_combined, start_date.isoformat()))

    if include_bus:
        for line_df in process_bus_days(start_date, end_date).values():
            weekly_data.extend(group_weekly_data(line_df, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(weekly_data, default=int), parse_float=Decimal), WEEKLY_TABLE_NAME)


def update_table(start_date: date, end_date: date, lines=constants.ALL_LINES, include_bus=True):
    """
    Update the table with rapid transit, commuter rail, and bus data
    """
    lamp_alerts_by_day = {}
    alert_requests = generate_requests(start_date, end_date, lines)
    all_data = process_requests(alert_requests, lines, lamp_alerts_by_day)

    grouped_data = []
    for line, df in all_data.items():
        grouped_data.extend(group_daily_data(df, start_date.isoformat()))

    # Aggregate commuter rail into a single "CommuterRail" entry
    cr_combined = aggregate_commuter_rail_df(all_data)
    if cr_combined is not None:
        grouped_data.extend(group_daily_data(cr_combined, start_date.isoformat()))

    if include_bus:
        for df in process_bus_days(start_date, end_date, lamp_alerts_by_day).values():
            grouped_data.extend(group_daily_data(df, start_date.isoformat()))

    dynamo.dynamo_batch_write(json.loads(json.dumps(grouped_data), parse_float=Decimal), DAILY_TABLE_NAME)


def backfill(start_date: date, end_date: date, lines=constants.ALL_LINES, include_bus=True):
    """Rebuild the daily and weekly tables a month at a time, so a long run can be
    resumed from the last month it printed. `lines=[]` backfills bus only."""
    chunk_start = start_date
    while chunk_start <= end_date:
        next_month = (chunk_start.replace(day=1) + timedelta(days=32)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end_date)
        print(f"Backfilling alert delays {chunk_start} to {chunk_end}")
        update_table(chunk_start, chunk_end, lines, include_bus)
        update_weekly_from_daily(chunk_start, chunk_end, lines, include_bus)
        chunk_start = next_month


# Usage: python -m chalicelib.delays.process START_DATE END_DATE  (YYYY-MM-DD)
if __name__ == "__main__":
    backfill(date.fromisoformat(sys.argv[1]), date.fromisoformat(sys.argv[2]))
