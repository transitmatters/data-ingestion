import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from ..car_ages import get_avg_car_age_for_line


def _make_response(data: list) -> MagicMock:
    mock = MagicMock()
    mock.content = json.dumps(data).encode("utf-8")
    mock.raise_for_status = MagicMock()
    return mock


# --- get_avg_car_age_for_line ---


def test_unknown_line_returns_none():
    result = get_avg_car_age_for_line(date(2025, 1, 1), "line-silver")
    assert result is None


def test_empty_api_response_returns_none():
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response([])
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-blue")
    assert result is None


def test_no_consist_and_no_label_returns_none():
    trips = [{"vehicle_consist": None, "vehicle_label": None}]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-blue")
    assert result is None


def test_consist_with_known_car_ids():
    # Blue line: cars 0700-0793 → built 2008
    # Two cars in consist: 700 and 750 → both 2008 → age = 2025 - 2008 = 17.0
    trips = [{"vehicle_consist": "700|750", "vehicle_label": None}]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-blue")
    assert result == Decimal("17.0")


def test_consist_with_unknown_car_ids_returns_none():
    # Car IDs outside any Blue line range
    trips = [{"vehicle_consist": "9999|8888", "vehicle_label": None}]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-blue")
    assert result is None


def test_vehicle_label_fallback():
    # No consist, but vehicle_label with known Orange line car
    # Orange: 1400-1415 → 2019; on date 2025, age = 2025 - 2019 = 6.0
    trips = [{"vehicle_consist": None, "vehicle_label": "1400"}]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-orange")
    assert result == Decimal("6.0")


def test_vehicle_label_multi_car_fallback():
    # vehicle_label with hyphen-separated IDs (multiple cars)
    # Orange: 1400 → 2019, 1416 → 2020; avg build year = 2019.5; age = 2025 - 2019.5 = 5.5
    trips = [{"vehicle_consist": None, "vehicle_label": "1400-1416"}]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-orange")
    assert result == Decimal("5.5")


def test_request_exception_returns_none():
    import requests as req

    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.side_effect = req.exceptions.ConnectionError("timeout")
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-red")
    assert result is None


def test_consist_with_invalid_car_id_strings_skipped():
    # Non-numeric entries in consist should be skipped gracefully
    trips = [{"vehicle_consist": "700|abc|750", "vehicle_label": None}]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-blue")
    # 700 and 750 are valid (both → 2008); "abc" skipped
    assert result == Decimal("17.0")


def test_multiple_trips_deduplicates_car_ids():
    # Same car 700 appears in two trips — set deduplication ensures it's counted once
    trips = [
        {"vehicle_consist": "700", "vehicle_label": None},
        {"vehicle_consist": "700", "vehicle_label": None},
    ]
    with patch("chalicelib.car_ages.requests.get") as mock_get:
        mock_get.return_value = _make_response(trips)
        result = get_avg_car_age_for_line(date(2025, 1, 1), "line-blue")
    assert result == Decimal("17.0")
