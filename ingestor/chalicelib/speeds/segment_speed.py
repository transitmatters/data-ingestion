from collections import namedtuple
from datetime import date, timedelta
from decimal import Decimal
from io import BytesIO
from itertools import pairwise
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import pathlib

import requests

from .. import dynamo
from .constants import BRANCHES, STATION_ID_MAP, COLORS, STATIONS


BUCKET = "tm-mbta-performance"

s3 = boto3.client("s3")

StopPair = namedtuple("StopPair", ["fr", "to"])
FullStopPair = namedtuple("FullStopPair", ["fr", "to", "fr_order", "to_order", "direction"])


# TODO: We're using SlowZones data for now, we may want our own dataset or to rename the SlowZones dataset
tt_prefix = "SlowZones/traveltimes/"
traveltimes = [x["Key"] for x in s3.list_objects(Bucket=BUCKET, Prefix=tt_prefix)["Contents"]]


def download(key: str | pathlib.Path):
    key = str(key)  # in case it's a pathlib.Path
    print("downloading", key)

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError:
        print(f"Missing data for {key}")
        return pd.DataFrame()

    buffer = BytesIO()
    buffer.write(obj["Body"].read())
    buffer.seek(0)
    df = pd.read_csv(buffer, compression="gzip", encoding="utf-8")

    df.service_date = pd.to_datetime(df.service_date).dt.date

    return df


def get_aggregate_data_dates(stop_pair: StopPair, ago: str, today: str, verbose=False, raw=False):
    # today and ago can be datetime.date or properly formatted str
    fr, to = stop_pair.fr, stop_pair.to
    url = f"https://dashboard-api.labs.transitmatters.org/api/aggregate/traveltimes?from_stop={fr}&to_stop={to}&start_date={ago}&end_date={today}"
    if verbose:
        print(url)
    resp = requests.get(url)

    if resp.status_code != 200:
        print("http status code:", resp.status_code, url)
        return None

    body = json.loads(resp.content)
    if raw:
        return body

    df = pd.DataFrame.from_records(body)
    df.columns = df.columns.str.strip()
    if df.empty:
        return df

    df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
    return df


def get_stop_pairs(color: str):
    return _get_stop_pairs(color, "0") + _get_stop_pairs(color, "1")


def _get_stop_pairs(color: str, direction: str):
    stop_objs = sorted(STATIONS[color]["stations"], key=lambda x: x["order"])
    if direction == "0":
        stop_objs.reverse()

    route_patterns = []
    branches = BRANCHES[color]

    if branches:
        for branch in branches:
            branch_stops = [x for x in stop_objs if branch in x["branches"]]
            route_patterns.append(branch_stops)
    else:
        route_patterns.append(stop_objs)

    pairs = {
        FullStopPair(fr["stops"][direction][0], to["stops"][direction][0], fr["order"], to["order"], direction)
        for rp in route_patterns
        for fr, to in pairwise(rp)
        if branches and set(fr["branches"]).intersection(to["branches"]) or not branches
    }
    return sorted(pairs, key=lambda x: (x.fr_order, x.to_order), reverse=(direction == "0"))


def format_tt_df(tts: pd.DataFrame | None, color: str, stop_pair: StopPair):
    if tts.empty:
        print("No data for", stop_pair)
        return tts

    tts.columns = tts.columns.str.strip()
    tts = tts.rename(columns={"service_date": "date"})
    tts["date"] = pd.to_datetime(tts["date"]).dt.date
    tts.set_index("date", inplace=True)

    tts = (tts["50%"]).dropna()
    tts = tts.rename("travel_time")
    tts = tts.to_frame()

    from_id = STATION_ID_MAP.get(stop_pair.fr, stop_pair.fr)
    to_id = STATION_ID_MAP.get(stop_pair.to, stop_pair.to)

    tts["route"] = color
    tts["from_id"] = from_id
    tts["to_id"] = to_id

    tts.index = tts.index.astype(str)
    tts.reset_index(inplace=True)  # Ensure service_date is included in the JSON output
    tts["date_stop_pair"] = tts["date"] + f"/{from_id}/{to_id}"

    return tts


def get_today_tts(color: str, stop_pair: StopPair):
    yesterday = date.today() - timedelta(days=1)
    today = date.today()

    tts = get_aggregate_data_dates(stop_pair, yesterday, today, verbose=True)

    return format_tt_df(tts, color, stop_pair)


def pair_from_fullpair(pair: FullStopPair):
    return StopPair(pair.fr, pair.to)


def daily_median_travel_time():
    pairs = [(color, pair) for color in COLORS for pair in get_stop_pairs(color)]

    row_dicts = pd.concat([get_today_tts(o[0], pair_from_fullpair(o[1])) for o in pairs]).to_dict(orient="records")

    unique_dict = {(item["date_stop_pair"], item["route"]): item for item in row_dicts}
    row_dicts = list(unique_dict.values())

    dynamo.dynamo_batch_write(json.loads(json.dumps(row_dicts), parse_float=Decimal), "SegmentTravelTimes")


if __name__ == "__main__":
    daily_median_travel_time()
