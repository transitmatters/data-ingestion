import pytest
from datetime import date

from ..service_ridership_dashboard.util import (
    bucket_by,
    date_from_string,
    date_range,
    date_range_contains,
    date_to_string,
    get_date_ranges_of_same_value,
    get_ranges_of_same_value,
    index_by,
)


# --- date_from_string / date_to_string ---


def test_date_from_string():
    assert date_from_string("2024-01-15") == date(2024, 1, 15)
    assert date_from_string("2000-12-31") == date(2000, 12, 31)


def test_date_to_string():
    assert date_to_string(date(2024, 1, 15)) == "2024-01-15"
    assert date_to_string(date(2000, 12, 31)) == "2000-12-31"


def test_date_round_trip():
    d = date(2024, 6, 15)
    assert date_from_string(date_to_string(d)) == d


# --- index_by ---


def test_index_by_string_key():
    items = [{"id": "a", "v": 1}, {"id": "b", "v": 2}]
    result = index_by(items, "id")
    assert result["a"] == {"id": "a", "v": 1}
    assert result["b"] == {"id": "b", "v": 2}


def test_index_by_callable_key():
    items = [10, 20, 30]
    result = index_by(items, lambda x: str(x))
    assert result["10"] == 10
    assert result["20"] == 20


def test_index_by_last_write_wins():
    items = [{"id": "x", "v": 1}, {"id": "x", "v": 2}]
    result = index_by(items, "id")
    assert result["x"]["v"] == 2


def test_index_by_empty():
    assert index_by([], "id") == {}


# --- bucket_by ---


def test_bucket_by_string_key():
    items = [{"type": "bus"}, {"type": "rail"}, {"type": "bus"}]
    result = bucket_by(items, "type")
    assert len(result["bus"]) == 2
    assert len(result["rail"]) == 1


def test_bucket_by_callable_key():
    items = [1, 2, 3, 4]
    result = bucket_by(items, lambda x: "even" if x % 2 == 0 else "odd")
    assert sorted(result["even"]) == [2, 4]
    assert sorted(result["odd"]) == [1, 3]


def test_bucket_by_empty():
    assert bucket_by([], "k") == {}


# --- get_ranges_of_same_value ---


def test_get_ranges_of_same_value_single_group():
    d = {1: "a", 2: "a", 3: "a"}
    result = list(get_ranges_of_same_value(d))
    assert result == [([1, 2, 3], "a")]


def test_get_ranges_of_same_value_multiple_groups():
    d = {1: "a", 2: "a", 3: "b", 4: "b", 5: "a"}
    result = list(get_ranges_of_same_value(d))
    assert result == [([1, 2], "a"), ([3, 4], "b"), ([5], "a")]


def test_get_ranges_of_same_value_all_different():
    d = {1: "a", 2: "b", 3: "c"}
    result = list(get_ranges_of_same_value(d))
    assert result == [([1], "a"), ([2], "b"), ([3], "c")]


def test_get_ranges_of_same_value_empty():
    assert list(get_ranges_of_same_value({})) == []


# --- get_date_ranges_of_same_value ---


def test_get_date_ranges_of_same_value():
    d = {
        date(2024, 1, 1): "x",
        date(2024, 1, 2): "x",
        date(2024, 1, 3): "y",
    }
    result = list(get_date_ranges_of_same_value(d))
    assert result == [
        ((date(2024, 1, 1), date(2024, 1, 2)), "x"),
        ((date(2024, 1, 3), date(2024, 1, 3)), "y"),
    ]


def test_get_date_ranges_of_same_value_single_entry():
    d = {date(2024, 3, 15): "z"}
    result = list(get_date_ranges_of_same_value(d))
    assert result == [((date(2024, 3, 15), date(2024, 3, 15)), "z")]


# --- date_range ---


def test_date_range_single():
    result = list(date_range(date(2024, 5, 1), date(2024, 5, 1)))
    assert result == [date(2024, 5, 1)]


def test_date_range_span():
    result = list(date_range(date(2024, 5, 1), date(2024, 5, 4)))
    assert result == [date(2024, 5, 1), date(2024, 5, 2), date(2024, 5, 3), date(2024, 5, 4)]


def test_date_range_raises_on_reversed():
    with pytest.raises(AssertionError):
        list(date_range(date(2024, 5, 4), date(2024, 5, 1)))


# --- date_range_contains ---


def test_date_range_contains_exact():
    r = (date(2024, 1, 1), date(2024, 12, 31))
    assert date_range_contains(r, r) is True


def test_date_range_contains_inner():
    outer = (date(2024, 1, 1), date(2024, 12, 31))
    inner = (date(2024, 3, 1), date(2024, 9, 30))
    assert date_range_contains(outer, inner) is True


def test_date_range_contains_partial_overlap():
    r1 = (date(2024, 1, 1), date(2024, 6, 30))
    r2 = (date(2024, 3, 1), date(2024, 9, 30))
    assert date_range_contains(r1, r2) is False


def test_date_range_contains_disjoint():
    r1 = (date(2024, 1, 1), date(2024, 3, 31))
    r2 = (date(2024, 6, 1), date(2024, 9, 30))
    assert date_range_contains(r1, r2) is False


def test_date_range_contains_boundary_start():
    outer = (date(2024, 1, 1), date(2024, 12, 31))
    contained = (date(2024, 1, 1), date(2024, 6, 30))
    assert date_range_contains(outer, contained) is True


def test_date_range_contains_boundary_end():
    outer = (date(2024, 1, 1), date(2024, 12, 31))
    contained = (date(2024, 6, 1), date(2024, 12, 31))
    assert date_range_contains(outer, contained) is True
