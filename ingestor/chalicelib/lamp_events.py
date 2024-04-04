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


def fetch_pq_file_from_remote(time_args: datetime) -> pd.DataFrame:
    service_date = _service_date(time_args)
    url = RAPID_DAILY_URL_TEMPLATE.format(YYYY_MM_DD=service_date.strftime("%Y-%m-%d"))
    result = requests.get(url)
    return pd.read_parquet(io.BytesIO(result.content), columns=INPUT_COLUMNS, engine="pyarrow")


def ingest_pq_file(pq_df: pd.DataFrame) -> pd.DataFrame:
    """Process and tranform columns for the full day's events."""
    # NB: We can trust df dtypes fetched from parquet files as the files are compressed with columnar metadata
    pq_df["dep_time"] = pd.to_datetime(pq_df["move_timestamp"], unit="s", utc=True).dt.tz_convert("US/Eastern")
    pq_df["arr_time"] = pd.to_datetime(pq_df["stop_timestamp"], unit="s", utc=True).dt.tz_convert("US/Eastern")
    pq_df["direction_id"] = pq_df["direction_id"].astype("int16")
    pq_df["service_date"] = pq_df["service_date"].apply(_format_dateint)

    # explode and stack dep and arr time
    arrivals_df = pq_df[pq_df["arr_time"].notna()]
    departures_df = pq_df[pq_df["dep_time"].notna()]
    arrivals_df["event_type"] = "ARR"
    arrivals_df["event_time"] = arrivals_df["arr_time"]
    departures_df["event_type"] = "DEP"
    departures_df["event_time"] = departures_df["dep_time"]

    # stitch together arrivals and departures
    processed_daily_events = pd.concat([arrivals_df, departures_df])

    # drop intermediate inference columns
    return processed_daily_events[OUTPUT_COLUMNS]


def make_s3_key(stop_id: str, time_args: datetime) -> str:
    service_date = _service_date(time_args)
    return S3_KEY_TEMPLATE.format(stop_id=stop_id, YYYY=service_date.year, _M=service_date.month, _D=service_date.day)


def upload_to_s3(stop_id_and_events: Tuple[str, pd.DataFrame], time_args: datetime) -> None:
    stop_id, stop_events = stop_id_and_events
    s3_key = make_s3_key(stop_id, time_args)
    # Upload s3 as DF
    print("Want to upload", s3_key)
    # s3.upload_df_as_csv(S3_BUCKET, s3_key, stop_events)


_parallel_upload = make_parallel(upload_to_s3)

if __name__ == "__main__":
    time_args = None or datetime.now(ZoneInfo(EASTERN_TIME))
    pq_df = fetch_pq_file_from_remote(time_args)
    processed_daily_events = ingest_pq_file(pq_df)

    # split daily events by stop_id and parallel upload to s3
    stop_event_groups = processed_daily_events.groupby("stop_id")
    _parallel_upload(stop_event_groups, time_args)
