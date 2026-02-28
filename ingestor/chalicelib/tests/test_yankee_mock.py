import json
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from ..yankee import (
    ShuttleTravelTime,
    TIME_FORMAT,
    get_driving_distance,
    load_bus_positions,
    maybe_create_travel_time,
    save_bus_positions,
    write_traveltimes_to_dynamo,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class FakeStop:
    stop_id: int
    stop_lon: float
    stop_lat: float


def _osrm_response(distance_meters: float, code: str = "Ok", status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.text = json.dumps({"code": code, "routes": [{"distance": distance_meters}]})
    return mock


# ---------------------------------------------------------------------------
# load_bus_positions
# ---------------------------------------------------------------------------


def test_load_bus_positions_returns_list():
    positions = [{"name": "Bus1", "latitude": 42.0, "longitude": -71.0}]
    with patch("chalicelib.yankee.s3.download") as mock_dl:
        mock_dl.return_value = json.dumps(positions)
        result = load_bus_positions()
    assert result == positions


def test_load_bus_positions_returns_none_on_no_such_key():
    err = ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject")
    with patch("chalicelib.yankee.s3.download", side_effect=err):
        result = load_bus_positions()
    assert result is None


def test_load_bus_positions_reraises_other_client_errors():
    err = ClientError({"Error": {"Code": "AccessDenied", "Message": "Forbidden"}}, "GetObject")
    with patch("chalicelib.yankee.s3.download", side_effect=err):
        with pytest.raises(ClientError):
            load_bus_positions()


def test_load_bus_positions_reraises_unexpected_exceptions():
    with patch("chalicelib.yankee.s3.download", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            load_bus_positions()


# ---------------------------------------------------------------------------
# save_bus_positions
# ---------------------------------------------------------------------------


def test_save_bus_positions_calls_upload():
    positions = [{"name": "Bus1"}]
    with patch("chalicelib.yankee.s3.upload") as mock_up:
        save_bus_positions(positions)
    mock_up.assert_called_once()
    args, kwargs = mock_up.call_args
    # First two positional args are bucket and key
    assert args[0] == "tm-shuttle-positions"
    assert args[1] == "yankee/last_shuttle_positions.csv"
    # The body is the JSON-encoded positions
    assert json.loads(args[2]) == positions


# ---------------------------------------------------------------------------
# write_traveltimes_to_dynamo
# ---------------------------------------------------------------------------


def test_write_traveltimes_to_dynamo_skips_none():
    with patch("chalicelib.yankee.dynamo.dynamo_batch_write") as mock_write:
        write_traveltimes_to_dynamo([None, None])
    mock_write.assert_called_once_with([], "ShuttleTravelTimes")


def test_write_traveltimes_to_dynamo_filters_none_entries():
    tt = ShuttleTravelTime(
        line="line-shuttle",
        routeId="Shuttle-AlewifeParkSt",
        date=datetime(2024, 6, 1),
        distance_miles=1.5,
        time=5.0,
        name="Bus42",
    )
    with patch("chalicelib.yankee.dynamo.dynamo_batch_write") as mock_write:
        write_traveltimes_to_dynamo([tt, None, tt])
    rows = mock_write.call_args[0][0]
    assert len(rows) == 2
    assert rows[0] == tt.__dict__


# ---------------------------------------------------------------------------
# get_driving_distance
# ---------------------------------------------------------------------------


def test_get_driving_distance_success():
    # 1000 meters * 0.000621371 = 0.621371 miles
    with patch("chalicelib.yankee.requests.get") as mock_get:
        mock_get.return_value = _osrm_response(1000.0)
        result = get_driving_distance((-71.0, 42.0), (-71.1, 42.1))
    assert result == pytest.approx(1000.0 * 0.000621371)


def test_get_driving_distance_non_200_returns_none():
    with patch("chalicelib.yankee.requests.get") as mock_get:
        mock_get.return_value = _osrm_response(0, status_code=500)
        result = get_driving_distance((-71.0, 42.0), (-71.1, 42.1))
    assert result is None


def test_get_driving_distance_non_ok_code_returns_none():
    with patch("chalicelib.yankee.requests.get") as mock_get:
        mock_get.return_value = _osrm_response(0, code="NoRoute")
        result = get_driving_distance((-71.0, 42.0), (-71.1, 42.1))
    assert result is None


def test_get_driving_distance_url_contains_coords():
    with patch("chalicelib.yankee.requests.get") as mock_get:
        mock_get.return_value = _osrm_response(500.0)
        get_driving_distance((-71.05, 42.36), (-71.06, 42.37))
    called_url = mock_get.call_args[0][0]
    assert "-71.05" in called_url
    assert "42.36" in called_url
    assert "-71.06" in called_url
    assert "42.37" in called_url


# ---------------------------------------------------------------------------
# maybe_create_travel_time
# ---------------------------------------------------------------------------

ROUTE_ID = "Shuttle-AlewifeParkSt"
BUS_NAME = "Bus99"


def test_maybe_create_travel_time_no_last_update_returns_none():
    stops = [FakeStop(1, -71.0, 42.0), FakeStop(2, -71.1, 42.1)]
    result = maybe_create_travel_time(BUS_NAME, ROUTE_ID, 1, 2, None, stops)
    assert result is None


def test_maybe_create_travel_time_stop_not_found_returns_none():
    # Stops have IDs 1 and 2, but we're asking for 3 → 4
    stops = [FakeStop(1, -71.0, 42.0), FakeStop(2, -71.1, 42.1)]
    last_update = datetime(2024, 1, 1, 8, 0, 0).strftime(TIME_FORMAT)
    result = maybe_create_travel_time(BUS_NAME, ROUTE_ID, 3, 4, last_update, stops)
    assert result is None


def test_maybe_create_travel_time_distance_none_returns_none():
    stops = [FakeStop(1, -71.0, 42.0), FakeStop(2, -71.1, 42.1)]
    last_update = datetime(2024, 1, 1, 8, 0, 0).strftime(TIME_FORMAT)
    with patch("chalicelib.yankee.get_driving_distance", return_value=None):
        result = maybe_create_travel_time(BUS_NAME, ROUTE_ID, 1, 2, last_update, stops)
    assert result is None


def test_maybe_create_travel_time_returns_shuttle_travel_time():
    stops = [FakeStop(1, -71.0, 42.0), FakeStop(2, -71.1, 42.1)]
    last_update = datetime(2024, 1, 1, 8, 0, 0).strftime(TIME_FORMAT)
    with patch("chalicelib.yankee.get_driving_distance", return_value=1.5):
        result = maybe_create_travel_time(BUS_NAME, ROUTE_ID, 1, 2, last_update, stops)
    assert isinstance(result, ShuttleTravelTime)
    assert result.line == "line-shuttle"
    assert result.routeId == ROUTE_ID
    assert result.name == BUS_NAME
    assert result.distance_miles == 1.5


def test_maybe_create_travel_time_calls_driving_distance_with_stop_coords():
    stops = [FakeStop(1, -71.0, 42.0), FakeStop(2, -71.1, 42.1)]
    last_update = datetime(2024, 1, 1, 8, 0, 0).strftime(TIME_FORMAT)
    with patch("chalicelib.yankee.get_driving_distance", return_value=2.0) as mock_dist:
        maybe_create_travel_time(BUS_NAME, ROUTE_ID, 1, 2, last_update, stops)
    mock_dist.assert_called_once_with((-71.0, 42.0), (-71.1, 42.1))
