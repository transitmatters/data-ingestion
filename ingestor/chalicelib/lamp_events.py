from datetime import date, datetime, timedelta
import io
import requests
from typing import Tuple
from zoneinfo import ZoneInfo

from parallel import make_parallel
import s3

import pandas as pd


LAMP_INDEX_URL = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/index.csv"
RAPID_DAILY_URL_TEMPLATE = "https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{YYYY_MM_DD}-subway-on-time-performance-v1.parquet"
S3_BUCKET = "datadashboard-backend-beta"
# month and day are not zero-padded
S3_KEY_TEMPLATE = "Events/daily-data/{stop_id}/Year={YYYY}/Month={_M}/Day={_D}/events.csv"
EASTERN_TIME = "US/Eastern"

# LAMP columns to fetch from parquet files
INPUT_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "stop_id",
    "direction_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "move_timestamp",  # departure time from the previous station
    "stop_timestamp",  # arrival time at the current station
]

# columns that should be output to s3 events.csv
OUTPUT_COLUMNS = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time",
]


def _format_dateint(dtint: int) -> str:
    """Safely takes a dateint of YYYYMMDD to YYYY-MM-DD."""
    return datetime.strptime(str(dtint), "%Y%m%d").strftime("%Y-%m-%d")


def _service_date(ts: datetime) -> date:
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=ZoneInfo(EASTERN_TIME))

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def fetch_pq_file_from_remote(service_date: date) -> pd.DataFrame:
    url = RAPID_DAILY_URL_TEMPLATE.format(YYYY_MM_DD=service_date.strftime("%Y-%m-%d"))
    result = requests.get(url)
    return pd.read_parquet(io.BytesIO(result.content), columns=INPUT_COLUMNS, engine="pyarrow")


def ingest_pq_file(pq_df: pd.DataFrame) -> pd.DataFrame:
    """Process and tranform columns for the full day's events."""
    # NB: While generally, we can trust df dtypes fetched from parquet files as the files are compressed with columnar metadata,
    # theres some numerical imprecisions that numpy seem to be throwing on M1 machines
    # that are affecting how epoch timestamps are being cased to datetimes. Maybe not a problem on the AWS machines, though?
    pq_df["dep_time"] = pd.to_datetime(pq_df["move_timestamp"], unit="s", utc=True).dt.tz_convert("US/Eastern")
    pq_df["arr_time"] = pd.to_datetime(pq_df["stop_timestamp"], unit="s", utc=True).dt.tz_convert("US/Eastern")
    pq_df["direction_id"] = pq_df["direction_id"].astype("int16")
    pq_df["service_date"] = pq_df["service_date"].apply(_format_dateint)

    # explode and stack departure and arrival times
    arr_df = pq_df[pq_df["arr_time"].notna()]
    arr_df = arr_df.assign(event_type="ARR").rename(columns={"arr_time": "event_time"}).drop(columns=["dep_time"])
    dep_df = pq_df[pq_df["dep_time"].notna()]
    dep_df = dep_df.assign(event_type="DEP").rename(columns={"dep_time": "event_time"}).drop(columns=["arr_time"])

    # stitch together arrivals and departures
    # TODO: sort by event_time?
    processed_daily_events = pd.concat([arr_df, dep_df])

    # drop intermediate inference columns
    return processed_daily_events[OUTPUT_COLUMNS]


def _local_save(S3_BUCKET, s3_key, stop_events):
    import os

    s3_key = ".temp/" + s3_key
    if not os.path.exists(os.path.dirname(s3_key)):
        os.makedirs(os.path.dirname(s3_key))
    stop_events.to_csv(s3_key)


def upload_to_s3(stop_id_and_events: Tuple[str, pd.DataFrame], service_date: date) -> None:
    # unpack from iterable
    stop_id, stop_events = stop_id_and_events

    # Upload to s3 as csv
    s3_key = S3_KEY_TEMPLATE.format(stop_id=stop_id, YYYY=service_date.year, _M=service_date.month, _D=service_date.day)
    # s3.upload_df_as_csv(S3_BUCKET, s3_key, stop_events)
    _local_save(S3_BUCKET, s3_key, stop_events)
    return [True]


_parallel_upload = make_parallel(upload_to_s3)

if __name__ == "__main__":
    time_args = None or datetime.now(ZoneInfo(EASTERN_TIME))
    service_date = _service_date(time_args)
    pq_df = fetch_pq_file_from_remote(service_date)
    processed_daily_events = ingest_pq_file(pq_df)

    # split daily events by stop_id and parallel upload to s3
    stop_event_groups = processed_daily_events.groupby("stop_id")
    a = _parallel_upload(stop_event_groups, service_date)
