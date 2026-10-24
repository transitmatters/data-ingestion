from ..service_ridership_dashboard.service_levels import (
    _divide_by_two_to_get_unidirectional_trip_counts,
    _get_has_service_exception,
    _get_trip_count_by_hour_totals_for_day,
)


def make_row(totals: list[int], has_service_exceptions: bool = False) -> dict:
    return {
        "byHour": {"totals": totals},
        "hasServiceExceptions": has_service_exceptions,
    }


# --- _divide_by_two_to_get_unidirectional_trip_counts ---


def test_divide_by_two_basic():
    result = _divide_by_two_to_get_unidirectional_trip_counts([4, 6, 2])
    assert result == [2.0, 3.0, 1.0]


def test_divide_by_two_empty():
    assert _divide_by_two_to_get_unidirectional_trip_counts([]) == []


def test_divide_by_two_odd_values():
    result = _divide_by_two_to_get_unidirectional_trip_counts([3])
    assert result == [1.5]


# --- _get_trip_count_by_hour_totals_for_day ---


def test_trip_count_single_row():
    rows = [make_row([2, 4, 6])]
    result = _get_trip_count_by_hour_totals_for_day(rows)
    assert result == [1.0, 2.0, 3.0]


def test_trip_count_two_routes_summed_then_halved():
    rows = [make_row([4, 2]), make_row([6, 8])]
    # Bidirectional totals: [10, 10] → halved: [5.0, 5.0]
    result = _get_trip_count_by_hour_totals_for_day(rows)
    assert result == [5.0, 5.0]


def test_trip_count_empty_rows():
    result = _get_trip_count_by_hour_totals_for_day([])
    assert result == []


# --- _get_has_service_exception ---


def test_has_service_exception_none():
    rows = [make_row([1, 2], has_service_exceptions=False)]
    assert _get_has_service_exception(rows) is False


def test_has_service_exception_one_true():
    rows = [
        make_row([1], has_service_exceptions=False),
        make_row([2], has_service_exceptions=True),
    ]
    assert _get_has_service_exception(rows) is True


def test_has_service_exception_all_true():
    rows = [make_row([1], has_service_exceptions=True)] * 3
    assert _get_has_service_exception(rows) is True


def test_has_service_exception_empty_list():
    assert _get_has_service_exception([]) is False
