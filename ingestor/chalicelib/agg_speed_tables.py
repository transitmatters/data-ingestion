from decimal import Decimal
import json
import numpy as np
from chalicelib import constants, dynamo
from chalice import BadRequestError
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from boto3.dynamodb.conditions import Key
import boto3
from dynamodb_json import json_util as ddb_json
import pandas as pd
import concurrent.futures

dynamodb = boto3.resource("dynamodb")


@dataclass
class Line:
    Literal["line-red", "line-orange", "line-blue", "line-green", "line-mattapan"]


@dataclass
class Range:
    Literal["weekly", "monthly", "daily"]


@dataclass
class TripsByLineParams:
    line: Line
    start_date: str
    end_date: str
    agg: Range


def populate_table(line: Line, range: Range, start_date: str = "2016-01-01"):
    """Populate weekly or monthly aggregate speed table for a given line. Ran manually as a lambda in AWS console"""
    print(f"Populating {range} table")
    table = constants.TABLE_MAP[range]
    today = datetime.now().strftime(constants.DATE_FORMAT_BACKEND)
    trips = actual_trips_by_line(
        {
            "start_date": start_date,
            "end_date": today,
            "line": line,
            "agg": range,
        }
    )
    dynamo.dynamo_batch_write(json.loads(json.dumps(trips), parse_float=Decimal), table["table_name"])
    print("Done")


def update_tables(range: Range):
    """Update weekly and monthly speed tables"""
    table = constants.TABLE_MAP[range]
    yesterday = datetime.now() - timedelta(days=1)
    for line in constants.LINES:
        start = table["update_start"]
        start_string = datetime.strftime(start, constants.DATE_FORMAT_BACKEND)
        end_string = datetime.strftime(yesterday, constants.DATE_FORMAT_BACKEND)
        print(f"Updating {line} for {range} for week of {start_string} to {end_string}")
        try:
            trips = actual_trips_by_line(
                {
                    "start_date": start_string,
                    "end_date": end_string,
                    "line": line,
                    "agg": range,
                }
            )
            dynamo.dynamo_batch_write(json.loads(json.dumps(trips), parse_float=Decimal), table["table_name"])
            print("Done")
        except Exception as e:
            print(e)


def query_daily_trips_on_route(table_name: str, route: str, start_date: str, end_date: str):
    table = dynamodb.Table(table_name)
    response = table.query(KeyConditionExpression=Key("route").eq(route) & Key("date").between(start_date, end_date))
    return ddb_json.loads(response["Items"])


def query_daily_trips_on_line(table_name: str, line: Line, start_date: str, end_date: str):
    route_keys = constants.LINE_TO_ROUTE_MAP[line]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(query_daily_trips_on_route, table_name, route_key, start_date, end_date)
            for route_key in route_keys
        ]
        results = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
    return results


def actual_trips_by_line(params: TripsByLineParams):
    try:
        start_date = params["start_date"]
        end_date = params["end_date"]
        line = params["line"]
        if line not in ["line-red", "line-blue", "line-green", "line-orange", "line-mattapan"]:
            raise BadRequestError("Invalid Line key.")
    except KeyError:
        raise BadRequestError("Missing or invalid parameters.")
    actual_trips = query_daily_trips_on_line("DeliveredTripMetrics", line, start_date, end_date)
    return aggregate_actual_trips(actual_trips, params["agg"], params["start_date"])


def aggregate_actual_trips(actual_trips, agg: Range, start_date: str):
    """Aggregate trips into lines and optionally week/month"""
    flat_data = [entry for sublist in actual_trips for entry in sublist]
    df = pd.DataFrame(flat_data)
    df_grouped = group_data_by_date_and_branch(df)
    if agg == "weekly":
        return group_weekly_data(df_grouped, start_date)
    if agg == "monthly":
        return group_monthly_data(df_grouped, start_date)
    return_data = df_grouped.reset_index()
    return return_data.to_dict(orient="records")


def group_monthly_data(df: pd.DataFrame, start_date: str):
    df_monthly = df.resample("M").agg(
        {"miles_covered": np.sum, "count": np.nanmedian, "total_time": np.sum, "line": "min"}
    )
    df_monthly = df_monthly.fillna(0)
    df_monthly.index = [datetime(x.year, x.month, 1) for x in df_monthly.index.tolist()]
    # Drop the first month if it is incomplete
    if datetime.fromisoformat(start_date).day != 1:
        df_monthly = df_monthly.tail(-1)
    df_monthly["date"] = df_monthly.index.strftime("%Y-%m-%d")
    return df_monthly.to_dict(orient="records")


def group_weekly_data(df: pd.DataFrame, start_date: str):
    # Group from Monday - Sunday
    df_weekly = df.resample("W-SUN").agg(
        {"miles_covered": np.sum, "count": np.nanmedian, "total_time": np.sum, "line": "min"}
    )
    df_weekly = df_weekly.fillna(0)
    # Pandas resample uses the end date of the range as the index. So we subtract 6 days to convert to first date of the range.
    df_weekly.index = df_weekly.index - pd.Timedelta(days=6)
    # Drop the first week if it is incomplete
    if datetime.fromisoformat(start_date).weekday() != 0:
        df_weekly = df_weekly.tail(-1)
    # Convert date back to string.
    df_weekly["date"] = df_weekly.index.strftime("%Y-%m-%d")
    return df_weekly.to_dict(orient="records")


def group_data_by_date_and_branch(df: pd.DataFrame):
    """Convert data from objects with specific route/date/direction to data by date."""
    # Set values for date to NaN when any entry for a different branch/direction has miles_covered as nan.
    df.loc[
        df.groupby("date")["miles_covered"].transform(lambda x: (np.isnan(x)).any()),
        ["count", "total_time", "miles_covered"],
    ] = np.nan
    # Aggregate valuues.
    df_grouped = (
        df.groupby("date")
        .agg(
            {
                "miles_covered": lambda x: np.nan if all(np.isnan(i) for i in x) else np.nansum(x),
                "total_time": lambda x: np.nan if all(np.isnan(i) for i in x) else np.nansum(x),
                "count": lambda x: np.nan if all(np.isnan(i) for i in x) else np.nansum(x),
                "line": "first",
            }
        )
        .reset_index()
    )
    # use datetime for index rather than string.
    df_grouped.set_index(pd.to_datetime(df_grouped["date"]), inplace=True)
    # Remove date column (it is the index.)
    df_grouped.drop("date", axis=1, inplace=True)
    return df_grouped
