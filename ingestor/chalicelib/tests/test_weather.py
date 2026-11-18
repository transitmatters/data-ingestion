from datetime import datetime

from ..date_utils import EASTERN_TIME
from ..weather.constants import WEATHER_CODE_TO_CONDITION, key
from ..weather.ingest import _drop_future_hours, _parse_hourly


HOURLY_FIXTURE = {
    "time": [
        "2026-04-22T00:00",
        "2026-04-22T01:00",
        "2026-04-23T00:00",
    ],
    "temperature_2m": [45.23, 44.1, 50.0],
    "weather_code": [0, 61, 75],
    "precipitation": [0.0, 0.05, 0.0],
    "relative_humidity_2m": [78, 82, 60],
    "wind_speed_10m": [8.47, 9.2, 5.0],
}


def test_parse_hourly_groups_by_date():
    by_date = _parse_hourly(HOURLY_FIXTURE)
    assert set(by_date.keys()) == {"2026-04-22", "2026-04-23"}
    assert set(by_date["2026-04-22"].keys()) == {"2026-04-22T00:00", "2026-04-22T01:00"}


def test_parse_hourly_record_shape():
    record = _parse_hourly(HOURLY_FIXTURE)["2026-04-22"]["2026-04-22T00:00"]
    assert record["temperature_f"] == 45.2
    assert record["weather_code"] == 0
    assert record["condition"] == "clear"
    assert record["precipitation_in"] == 0.0
    assert record["humidity_pct"] == 78
    assert record["wind_mph"] == 8.5


def test_weather_code_mapping():
    assert WEATHER_CODE_TO_CONDITION[0] == "clear"
    assert WEATHER_CODE_TO_CONDITION[3] == "cloudy"
    assert WEATHER_CODE_TO_CONDITION[45] == "fog"
    assert WEATHER_CODE_TO_CONDITION[63] == "rain"
    assert WEATHER_CODE_TO_CONDITION[75] == "snow"
    assert WEATHER_CODE_TO_CONDITION[95] == "storm"


def test_unknown_code_falls_back():
    fixture = {
        "time": ["2026-04-22T00:00"],
        "temperature_2m": [40.0],
        "weather_code": [999],
        "precipitation": [0.0],
        "relative_humidity_2m": [50],
        "wind_speed_10m": [0.0],
    }
    record = _parse_hourly(fixture)["2026-04-22"]["2026-04-22T00:00"]
    assert record["condition"] == "unknown"


def test_merge_preserves_other_hours_and_overwrites_same_hour():
    existing = {
        "2026-04-22T00:00": {"temperature_f": 40.0, "condition": "clear"},
        "2026-04-22T01:00": {"temperature_f": 39.0, "condition": "clear"},
    }
    new = _parse_hourly(
        {
            "time": ["2026-04-22T01:00", "2026-04-22T02:00"],
            "temperature_2m": [41.0, 42.0],
            "weather_code": [61, 2],
            "precipitation": [0.1, 0.0],
            "relative_humidity_2m": [80, 75],
            "wind_speed_10m": [7.0, 6.0],
        }
    )["2026-04-22"]

    existing.update(new)

    assert existing["2026-04-22T00:00"]["temperature_f"] == 40.0
    assert existing["2026-04-22T01:00"]["condition"] == "rain"
    assert existing["2026-04-22T01:00"]["temperature_f"] == 41.0
    assert existing["2026-04-22T02:00"]["condition"] == "cloudy"


def test_key_format():
    assert key("2026-04-22") == "Weather/hourly/2026-04-22.json.gz"


def test_drop_future_hours_keeps_current_and_past():
    by_date = {
        "2026-04-22": {"2026-04-22T13:00": {}, "2026-04-22T14:00": {}, "2026-04-22T15:00": {}},
        "2026-04-21": {"2026-04-21T23:00": {}},
    }
    now = datetime(2026, 4, 22, 14, 37, tzinfo=EASTERN_TIME)
    kept = _drop_future_hours(by_date, now)
    assert set(kept["2026-04-22"]) == {"2026-04-22T13:00", "2026-04-22T14:00"}
    assert set(kept["2026-04-21"]) == {"2026-04-21T23:00"}


def test_drop_future_hours_drops_days_with_only_forecasts():
    by_date = {
        "2026-04-22": {"2026-04-22T23:00": {}},
        "2026-04-23": {"2026-04-23T00:00": {}, "2026-04-23T01:00": {}},
    }
    now = datetime(2026, 4, 22, 23, 5, tzinfo=EASTERN_TIME)
    assert set(_drop_future_hours(by_date, now)) == {"2026-04-22"}
