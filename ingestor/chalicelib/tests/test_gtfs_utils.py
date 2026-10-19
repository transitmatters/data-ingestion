import pytest
from datetime import date
from types import SimpleNamespace

from ..gtfs.utils import (
    bucket_by,
    bucket_trips_by_hour,
    date_range,
    get_total_service_minutes,
    index_by,
    is_valid_route_id,
)


def make_trip(start_time: int, end_time: int):
    return SimpleNamespace(start_time=start_time, end_time=end_time)


# --- bucket_trips_by_hour ---


def test_bucket_trips_by_hour_empty():
    assert bucket_trips_by_hour([]) == [0] * 24


def test_bucket_trips_by_hour_single_trip():
    trip = make_trip(start_time=9 * 3600, end_time=10 * 3600)  # 9:00 AM
    result = bucket_trips_by_hour([trip])
    assert result[9] == 1
    assert sum(result) == 1


def test_bucket_trips_by_hour_multiple_trips():
    trips = [
        make_trip(0 * 3600, 1 * 3600),  # midnight
        make_trip(0 * 3600 + 30 * 60, 2 * 3600),  # 12:30 AM, still hour 0
        make_trip(8 * 3600, 9 * 3600),  # 8 AM
        make_trip(23 * 3600, 24 * 3600),  # 11 PM
    ]
    result = bucket_trips_by_hour(trips)
    assert result[0] == 2
    assert result[8] == 1
    assert result[23] == 1
    assert sum(result) == 4


def test_bucket_trips_by_hour_wraps_at_24():
    # start_time >= 24 * 3600 (next service day) wraps via % 24
    trip = make_trip(start_time=25 * 3600, end_time=26 * 3600)  # 1 AM of next day → hour 1
    result = bucket_trips_by_hour([trip])
    assert result[1] == 1


# --- get_total_service_minutes ---


def test_get_total_service_minutes_empty():
    assert get_total_service_minutes([]) == 0


def test_get_total_service_minutes_single_trip():
    trip = make_trip(0, 3600)  # 60 minutes
    assert get_total_service_minutes([trip]) == 60


def test_get_total_service_minutes_multiple_trips():
    trips = [
        make_trip(0, 1800),  # 30 min
        make_trip(0, 3600),  # 60 min
        make_trip(0, 7200),  # 120 min
    ]
    assert get_total_service_minutes(trips) == 210


def test_get_total_service_minutes_truncates_to_int():
    trip = make_trip(0, 90)  # 1.5 minutes → truncates to 1
    assert get_total_service_minutes([trip]) == 1


# --- is_valid_route_id ---


def test_is_valid_route_id_normal_routes():
    assert is_valid_route_id("Red") is True
    assert is_valid_route_id("Orange") is True
    assert is_valid_route_id("Blue") is True
    assert is_valid_route_id("Green-B") is True
    assert is_valid_route_id("CR-Framingham") is True


def test_is_valid_route_id_shuttle_routes():
    assert is_valid_route_id("Shuttle-RedGreenHealth") is False
    assert is_valid_route_id("Shuttle-") is False
    assert is_valid_route_id("ShuttleNotActuallyShuttle") is False  # starts with "Shuttle"


def test_is_valid_route_id_green_line_shuttle():
    assert is_valid_route_id("602") is False


# --- bucket_by ---


def test_bucket_by_string_key():
    items = [{"color": "red"}, {"color": "blue"}, {"color": "red"}]
    result = bucket_by(items, "color")
    assert len(result["red"]) == 2
    assert len(result["blue"]) == 1


def test_bucket_by_callable_key():
    items = [1, 2, 3, 4, 5]
    result = bucket_by(items, lambda x: "even" if x % 2 == 0 else "odd")
    assert sorted(result["even"]) == [2, 4]
    assert sorted(result["odd"]) == [1, 3, 5]


def test_bucket_by_empty():
    assert bucket_by([], "key") == {}


def test_bucket_by_preserves_order():
    items = [{"k": "a"}, {"k": "b"}, {"k": "a"}]
    result = bucket_by(items, "k")
    assert result["a"] == [{"k": "a"}, {"k": "a"}]


# --- index_by ---


def test_index_by_string_key():
    items = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    result = index_by(items, "id")
    assert result[1] == {"id": 1, "val": "a"}
    assert result[2] == {"id": 2, "val": "b"}


def test_index_by_callable_key():
    items = ["foo", "bar", "baz"]
    result = index_by(items, lambda s: s[0])
    assert result["f"] == "foo"
    assert result["b"] == "baz"  # last write wins


def test_index_by_last_write_wins():
    items = [{"id": 1, "val": "first"}, {"id": 1, "val": "second"}]
    result = index_by(items, "id")
    assert result[1]["val"] == "second"


def test_index_by_empty():
    assert index_by([], "key") == {}


# --- date_range ---


def test_date_range_single_day():
    result = list(date_range(date(2024, 1, 1), date(2024, 1, 1)))
    assert result == [date(2024, 1, 1)]


def test_date_range_multiple_days():
    result = list(date_range(date(2024, 1, 1), date(2024, 1, 5)))
    assert result == [
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    ]


def test_date_range_across_month_boundary():
    result = list(date_range(date(2024, 1, 30), date(2024, 2, 2)))
    assert result == [date(2024, 1, 30), date(2024, 1, 31), date(2024, 2, 1), date(2024, 2, 2)]


def test_date_range_raises_on_invalid():
    with pytest.raises(AssertionError):
        list(date_range(date(2024, 1, 5), date(2024, 1, 1)))
