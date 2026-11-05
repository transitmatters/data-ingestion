"""Tests for the service ridership dashboard's thread-pooled queries and the daily trip metrics job.

These check pagination, and that the output matches the one-at-a-time path.
"""

import json
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
import requests

from .. import constants, daily_speeds
from ..service_ridership_dashboard import queries, service_levels

# ---------------------------------------------------------------------------
# queries._query_all
# ---------------------------------------------------------------------------


class _PagedTable:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def query(self, **kwargs):
        self.calls.append(kwargs)
        page = self.pages[len(self.calls) - 1]
        response = {"Items": page["items"]}
        if "next" in page:
            response["LastEvaluatedKey"] = page["next"]
        return response


def test_query_all_follows_last_evaluated_key():
    table = _PagedTable(
        [
            {"items": [{"date": "2024-01-01"}], "next": {"date": "2024-01-01"}},
            {"items": [{"date": "2024-01-02"}]},
        ]
    )

    items = queries._query_all(table, KeyConditionExpression="cond")

    assert items == [{"date": "2024-01-01"}, {"date": "2024-01-02"}]
    assert table.calls[1]["ExclusiveStartKey"] == {"date": "2024-01-01"}
    assert table.calls[1]["KeyConditionExpression"] == "cond"


def test_query_all_single_page():
    table = _PagedTable([{"items": [{"date": "2024-01-01"}]}])
    assert queries._query_all(table, KeyConditionExpression="cond") == [{"date": "2024-01-01"}]
    assert len(table.calls) == 1


# ---------------------------------------------------------------------------
# service_levels.get_service_level_entries_by_line_id
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Line:
    line_id: str
    line_short_name: str
    line_long_name: str


def _line(line_id):
    return _Line(line_id, line_id, f"{line_id} long")


def _route(route_id):
    return SimpleNamespace(route_id=route_id)


def test_service_levels_combine_routes_per_line(monkeypatch):
    # Each route runs (n) trips in every hour, where n is fixed per route.
    trips_per_hour = {"r1": 2, "r2": 4, "r3": 10}

    def fake_query(start_date, end_date, route_id):
        return [
            {"date": d.isoformat(), "byHour": {"totals": [trips_per_hour[route_id]] * 24}}
            for d in (start_date, start_date + timedelta(days=1))
        ]

    monkeypatch.setattr(service_levels, "query_scheduled_service", fake_query)
    # r2 is shared by both lines, to check each line still gets its own copy of the rows.
    routes_by_line = {
        _line("line-A"): [_route("r1"), _route("r2")],
        _line("line-B"): [_route("r2"), _route("r3")],
    }

    result = service_levels.get_service_level_entries_by_line_id(routes_by_line, date(2024, 1, 1), date(2024, 1, 2))

    assert set(result) == {"line-A", "line-B"}
    for day in (date(2024, 1, 1), date(2024, 1, 2)):
        # bidirectional totals summed across the line's routes, then halved
        assert result["line-A"][day].service_levels == [(2 + 4) / 2] * 24
        assert result["line-B"][day].service_levels == [(4 + 10) / 2] * 24
        assert result["line-A"][day].route_ids == ["r1", "r2"]


# ---------------------------------------------------------------------------
# daily_speeds.update_daily_table
# ---------------------------------------------------------------------------

DAY = date(2026, 11, 3)
ROUTES = [("line-red", "a"), ("line-orange", None), ("line-blue", None)]


class _Response:
    def __init__(self, url, status=200):
        self.url = url
        self.status_code = status
        # A deterministic payload per URL, so routes and requests are distinguishable.
        seed = sum(map(ord, url)) % 97
        self.content = json.dumps(
            [{"service_date": DAY.isoformat(), "50%": seed + 0.5, "mean": seed + 0.25, "count": seed * 2}]
        ).encode()

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.exceptions.HTTPError(self.url)


def _serial_reference(fleet_metrics):
    """The expected rows: one route, one request at a time."""
    expected = []
    date_string = DAY.strftime(constants.DATE_FORMAT_BACKEND)
    for line, route in ROUTES:
        route_metadata = constants.get_route_metadata(line, DAY, False, route)
        api_requests = daily_speeds.get_agg_tt_api_requests(route_metadata["stops"], DAY, timedelta(days=1))
        speed_object = daily_speeds.send_requests(api_requests)
        formatted = daily_speeds.format_tt_objects(
            speed_object, route_metadata, line, route, len(api_requests), [date_string]
        )
        for obj in formatted:
            obj.update(fleet_metrics[line])
        expected.extend(formatted)
    return expected


def test_update_daily_table_matches_serial_path(monkeypatch):
    fleet_metrics = {line: {"avg_car_age": Decimal(len(line))} for line, _ in ROUTES}
    monkeypatch.setattr(requests, "get", lambda url, **kwargs: _Response(url))
    monkeypatch.setattr(daily_speeds, "get_fleet_age_metrics_for_line", lambda d, line: fleet_metrics[line])
    written = []
    monkeypatch.setattr(daily_speeds.dynamo, "dynamo_batch_write", lambda objs, table: written.append((objs, table)))

    daily_speeds.update_daily_table(DAY, routes=ROUTES)

    assert len(written) == 1
    objs, table = written[0]
    assert table == "DeliveredTripMetrics"
    assert objs == _serial_reference(fleet_metrics)
    assert len(objs) == len(ROUTES)


def test_update_daily_table_raises_on_http_error_without_writing(monkeypatch):
    def flaky_get(url, **kwargs):
        return _Response(url, status=500 if "70064" in url else 200)

    monkeypatch.setattr(requests, "get", flaky_get)
    monkeypatch.setattr(daily_speeds, "get_fleet_age_metrics_for_line", lambda d, line: None)
    written = []
    monkeypatch.setattr(daily_speeds.dynamo, "dynamo_batch_write", lambda objs, table: written.append(objs))

    with pytest.raises(requests.exceptions.HTTPError):
        daily_speeds.update_daily_table(DAY, routes=ROUTES)
    assert written == []
