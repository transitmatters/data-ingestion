import json
from datetime import date
from unittest.mock import MagicMock, patch

from ..delays.process import process_single_day
from ..delays.types import AlertsRequest


def _make_response(data: list, status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.content = json.dumps(data).encode("utf-8")
    mock.status_code = status_code
    mock.raise_for_status = MagicMock()
    return mock


# --- process_single_day ---


def test_process_single_day_returns_list():
    alerts = [
        {"valid_from": "2024-01-01T08:00:00", "valid_to": "2024-01-01T09:00:00", "text": "No issues today"}
    ]
    request = AlertsRequest(route="Red", date=date(2024, 1, 1))
    with patch("chalicelib.delays.process.requests.get") as mock_get:
        mock_get.return_value = _make_response(alerts)
        result = process_single_day(request)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["text"] == "No issues today"


def test_process_single_day_empty_response():
    request = AlertsRequest(route="Blue", date=date(2024, 3, 15))
    with patch("chalicelib.delays.process.requests.get") as mock_get:
        mock_get.return_value = _make_response([])
        result = process_single_day(request)
    assert result == []


def test_process_single_day_calls_correct_route():
    request = AlertsRequest(route="Orange", date=date(2024, 6, 1))
    with patch("chalicelib.delays.process.requests.get") as mock_get:
        mock_get.return_value = _make_response([])
        process_single_day(request)
        called_url = mock_get.call_args[0][0]
    assert "Orange" in called_url
    assert "2024-06-01" in called_url


def test_process_single_day_raises_on_http_error():
    import requests as req

    request = AlertsRequest(route="Red", date=date(2024, 1, 1))
    mock_resp = _make_response([], status_code=500)
    mock_resp.raise_for_status.side_effect = req.exceptions.HTTPError("500 Server Error")
    with patch("chalicelib.delays.process.requests.get") as mock_get:
        mock_get.return_value = mock_resp
        try:
            process_single_day(request)
            raised = False
        except req.exceptions.HTTPError:
            raised = True
    assert raised
