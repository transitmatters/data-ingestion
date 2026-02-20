"""
Integration tests for service_ridership_dashboard/queries.py and ridership.py
using a moto-backed DynamoDB.

Both modules wrap boto3 DynamoDB queries; these tests seed data directly into
the moto tables and verify that the query functions return correctly shaped and
filtered results.
"""

from datetime import date

import pytest

from ..service_ridership_dashboard.queries import query_ridership, query_scheduled_service
from ..service_ridership_dashboard.ridership import RidershipEntry, _get_ridership_for_line_id, ridership_by_line_id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ssd_row(route_id: str, entry_date: str, totals: list[int] | None = None) -> dict:
    """Build a minimal ScheduledServiceDaily row."""
    return {
        "routeId": route_id,
        "date": entry_date,
        "byHour": {"totals": totals or ([0] * 24)},
        "count": sum(totals or []),
        "hasServiceExceptions": False,
        "lineId": f"line-{route_id}",
        "serviceMinutes": 120,
        "timestamp": 1705276800,
    }


def _ridership_row(line_id: str, entry_date: str, count: int) -> dict:
    return {
        "lineId": line_id,
        "date": entry_date,
        "count": count,
        "timestamp": 1705276800,
    }


# ---------------------------------------------------------------------------
# query_scheduled_service
# ---------------------------------------------------------------------------


def test_query_scheduled_service_returns_rows_in_range(dynamodb_tables):
    dynamodb_tables["ScheduledServiceDaily"].put_item(Item=_ssd_row("Red", "2024-01-15"))
    dynamodb_tables["ScheduledServiceDaily"].put_item(Item=_ssd_row("Red", "2024-01-20"))
    dynamodb_tables["ScheduledServiceDaily"].put_item(Item=_ssd_row("Red", "2024-02-01"))  # out of range

    results = query_scheduled_service(date(2024, 1, 1), date(2024, 1, 31), "Red")

    assert len(results) == 2
    dates = {r["date"] for r in results}
    assert dates == {"2024-01-15", "2024-01-20"}


def test_query_scheduled_service_filters_by_route(dynamodb_tables):
    dynamodb_tables["ScheduledServiceDaily"].put_item(Item=_ssd_row("Red", "2024-01-15"))
    dynamodb_tables["ScheduledServiceDaily"].put_item(Item=_ssd_row("Blue", "2024-01-15"))

    results = query_scheduled_service(date(2024, 1, 1), date(2024, 1, 31), "Red")

    assert len(results) == 1
    assert results[0]["routeId"] == "Red"


def test_query_scheduled_service_returns_empty_when_no_data(dynamodb_tables):
    results = query_scheduled_service(date(2024, 1, 1), date(2024, 1, 31), "Orange")
    assert results == []


def test_query_scheduled_service_row_has_expected_fields(dynamodb_tables):
    hour_totals = [0] * 6 + [2, 4, 6, 8, 6, 4, 4, 4, 6, 8, 6, 4, 2, 0] + [0] * 4
    dynamodb_tables["ScheduledServiceDaily"].put_item(Item=_ssd_row("Red", "2024-01-15", hour_totals))

    results = query_scheduled_service(date(2024, 1, 1), date(2024, 1, 31), "Red")

    row = results[0]
    assert row["routeId"] == "Red"
    assert row["date"] == "2024-01-15"
    assert "byHour" in row
    assert len(row["byHour"]["totals"]) == 24


# ---------------------------------------------------------------------------
# query_ridership
# ---------------------------------------------------------------------------


def test_query_ridership_returns_rows_in_range(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-10", 5000))
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-20", 4800))
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-02-05", 4600))  # out of range

    results = query_ridership(date(2024, 1, 1), date(2024, 1, 31), "line-Red")

    assert len(results) == 2
    dates = {r["date"] for r in results}
    assert dates == {"2024-01-10", "2024-01-20"}


def test_query_ridership_filters_by_line(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-10", 5000))
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Blue", "2024-01-10", 2000))

    results = query_ridership(date(2024, 1, 1), date(2024, 1, 31), "line-Red")

    assert len(results) == 1
    assert results[0]["lineId"] == "line-Red"


def test_query_ridership_returns_empty_when_no_data(dynamodb_tables):
    results = query_ridership(date(2024, 1, 1), date(2024, 1, 31), "line-Orange")
    assert results == []


# ---------------------------------------------------------------------------
# _get_ridership_for_line_id
# ---------------------------------------------------------------------------


def test_get_ridership_for_line_id_returns_entries_keyed_by_date(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-10", 5000))
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-15", 4800))

    result = _get_ridership_for_line_id(date(2024, 1, 1), date(2024, 1, 31), "line-Red")

    assert len(result) == 2
    assert date(2024, 1, 10) in result
    assert date(2024, 1, 15) in result


def test_get_ridership_for_line_id_creates_ridership_entries(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-10", 5000))

    result = _get_ridership_for_line_id(date(2024, 1, 1), date(2024, 1, 31), "line-Red")

    entry = result[date(2024, 1, 10)]
    assert isinstance(entry, RidershipEntry)
    assert entry.date == date(2024, 1, 10)
    assert int(entry.ridership) == 5000


def test_get_ridership_for_line_id_returns_empty_dict_when_no_data(dynamodb_tables):
    result = _get_ridership_for_line_id(date(2024, 1, 1), date(2024, 1, 31), "line-Orange")
    assert result == {}


# ---------------------------------------------------------------------------
# ridership_by_line_id
# ---------------------------------------------------------------------------


def test_ridership_by_line_id_returns_data_for_each_line(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Red", "2024-01-10", 5000))
    dynamodb_tables["Ridership"].put_item(Item=_ridership_row("line-Blue", "2024-01-10", 2000))

    result = ridership_by_line_id(date(2024, 1, 1), date(2024, 1, 31), ["line-Red", "line-Blue"])

    assert set(result.keys()) == {"line-Red", "line-Blue"}
    assert date(2024, 1, 10) in result["line-Red"]
    assert date(2024, 1, 10) in result["line-Blue"]


def test_ridership_by_line_id_returns_empty_dict_for_missing_line(dynamodb_tables):
    result = ridership_by_line_id(date(2024, 1, 1), date(2024, 1, 31), ["line-Orange"])
    assert result == {"line-Orange": {}}
