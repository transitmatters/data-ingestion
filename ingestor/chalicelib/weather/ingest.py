import json
from datetime import date, datetime, timedelta

import requests
from botocore.exceptions import ClientError

from chalicelib import s3
from chalicelib.date_utils import get_current_service_date
from chalicelib.weather.constants import (
    ARCHIVE_URL,
    BUCKET,
    FORECAST_URL,
    HOURLY_FIELDS,
    LATITUDE,
    LONGITUDE,
    WEATHER_CODE_TO_CONDITION,
    key,
)

# Open-Meteo archive lag: observations are typically published with a ~2 day delay.
ARCHIVE_LAG_DAYS = 2
# Keep each archive request bounded so timeouts & memory stay predictable.
BACKFILL_CHUNK_DAYS = 31


def _base_params():
    return {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": HOURLY_FIELDS,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/New_York",
    }


def _round(value, digits):
    if value is None:
        return None
    return round(value, digits)


def _parse_hourly(hourly):
    """Turn the parallel-array Open-Meteo response into {iso_hour: record} dicts, grouped by date."""
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    codes = hourly.get("weather_code", [])
    precip = hourly.get("precipitation", [])
    humidity = hourly.get("relative_humidity_2m", [])
    wind = hourly.get("wind_speed_10m", [])

    by_date = {}
    for i, ts in enumerate(times):
        code = codes[i] if i < len(codes) else None
        record = {
            "temperature_f": _round(temps[i] if i < len(temps) else None, 1),
            "weather_code": code,
            "condition": WEATHER_CODE_TO_CONDITION.get(code, "unknown"),
            "precipitation_in": _round(precip[i] if i < len(precip) else None, 2),
            "humidity_pct": humidity[i] if i < len(humidity) else None,
            "wind_mph": _round(wind[i] if i < len(wind) else None, 1),
        }
        day = ts[:10]
        by_date.setdefault(day, {})[ts] = record
    return by_date


def _read_day(day):
    try:
        existing = s3.download(BUCKET, key(day), encoding="utf8", compressed=True)
        return json.loads(existing)
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
        return {}


def _write_day(day, entries):
    payload = json.dumps(entries).encode("utf8")
    s3.upload(BUCKET, key(day), payload, compress=True)


def ingest_hourly_weather():
    """Fetch the latest hourly weather and merge into today's S3 file.

    Requests past_days=1 + forecast_days=1 so gaps from a skipped run get backfilled on the next invocation.
    """
    params = {**_base_params(), "past_days": 1, "forecast_days": 1}
    response = requests.get(FORECAST_URL, params=params, timeout=20)
    response.raise_for_status()
    by_date = _parse_hourly(response.json().get("hourly", {}))

    service_date = get_current_service_date()
    for day, entries in by_date.items():
        existing = _read_day(day)
        existing.update(entries)
        _write_day(day, existing)

    return service_date


def _parse_date(value):
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _chunks(start, end, size):
    cur = start
    step = timedelta(days=size - 1)
    while cur <= end:
        chunk_end = min(cur + step, end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def backfill_weather(start_date, end_date):
    """Pull historical hourly weather from Open-Meteo's archive and write one daily file per covered date."""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    latest_available = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    if end > latest_available:
        end = latest_available
    if start > end:
        return 0

    written = 0
    for chunk_start, chunk_end in _chunks(start, end, BACKFILL_CHUNK_DAYS):
        params = {
            **_base_params(),
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
        }
        response = requests.get(ARCHIVE_URL, params=params, timeout=60)
        response.raise_for_status()
        by_date = _parse_hourly(response.json().get("hourly", {}))

        for day, entries in by_date.items():
            existing = _read_day(day)
            existing.update(entries)
            _write_day(day, existing)
            written += 1

    return written
