"""
Integration tests for dynamo.py using a moto-backed DynamoDB.

Tests cover:
  dynamo_batch_write  — writes items, respects empty-list early-exit
  query_dynamo        — returns items matching a key condition
"""

from boto3.dynamodb.conditions import Key

from ..dynamo import dynamo_batch_write, query_dynamo


# ---------------------------------------------------------------------------
# dynamo_batch_write
# ---------------------------------------------------------------------------


def test_dynamo_batch_write_puts_items(dynamodb_tables):
    items = [
        {"routeId": "Red", "date": "2024-01-15", "count": 100},
        {"routeId": "Red", "date": "2024-01-16", "count": 80},
    ]
    dynamo_batch_write(items, "ScheduledServiceDaily")

    stored = dynamodb_tables["ScheduledServiceDaily"].scan()["Items"]
    assert len(stored) == 2
    route_ids = {item["routeId"] for item in stored}
    assert route_ids == {"Red"}


def test_dynamo_batch_write_multiple_routes(dynamodb_tables):
    items = [
        {"routeId": "Red", "date": "2024-01-15", "count": 100},
        {"routeId": "Blue", "date": "2024-01-15", "count": 50},
    ]
    dynamo_batch_write(items, "ScheduledServiceDaily")

    stored = dynamodb_tables["ScheduledServiceDaily"].scan()["Items"]
    assert len(stored) == 2
    assert {i["routeId"] for i in stored} == {"Red", "Blue"}


def test_dynamo_batch_write_empty_list_is_noop(dynamodb_tables):
    dynamo_batch_write([], "ScheduledServiceDaily")

    assert dynamodb_tables["ScheduledServiceDaily"].scan()["Count"] == 0


def test_dynamo_batch_write_overwrites_on_same_key(dynamodb_tables):
    dynamo_batch_write([{"routeId": "Red", "date": "2024-01-15", "count": 100}], "ScheduledServiceDaily")
    dynamo_batch_write([{"routeId": "Red", "date": "2024-01-15", "count": 999}], "ScheduledServiceDaily")

    stored = dynamodb_tables["ScheduledServiceDaily"].scan()["Items"]
    assert len(stored) == 1
    assert int(stored[0]["count"]) == 999


# ---------------------------------------------------------------------------
# query_dynamo
# ---------------------------------------------------------------------------


def test_query_dynamo_returns_matching_items(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item={"lineId": "line-Red", "date": "2024-01-15", "count": 5000})
    dynamodb_tables["Ridership"].put_item(Item={"lineId": "line-Red", "date": "2024-01-16", "count": 4500})
    dynamodb_tables["Ridership"].put_item(Item={"lineId": "line-Blue", "date": "2024-01-15", "count": 2000})

    params = {
        "KeyConditionExpression": Key("lineId").eq("line-Red") & Key("date").between("2024-01-01", "2024-01-31"),
    }
    results = query_dynamo(params, "Ridership")

    assert len(results) == 2
    assert all(item["lineId"] == "line-Red" for item in results)


def test_query_dynamo_returns_empty_when_no_match(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item={"lineId": "line-Red", "date": "2024-01-15", "count": 5000})

    params = {
        "KeyConditionExpression": Key("lineId").eq("line-Orange") & Key("date").between("2024-01-01", "2024-01-31"),
    }
    results = query_dynamo(params, "Ridership")

    assert results == []


def test_query_dynamo_respects_date_range(dynamodb_tables):
    dynamodb_tables["Ridership"].put_item(Item={"lineId": "line-Red", "date": "2024-01-10", "count": 1})
    dynamodb_tables["Ridership"].put_item(Item={"lineId": "line-Red", "date": "2024-02-10", "count": 2})

    params = {
        "KeyConditionExpression": Key("lineId").eq("line-Red") & Key("date").between("2024-01-01", "2024-01-31"),
    }
    results = query_dynamo(params, "Ridership")

    assert len(results) == 1
    assert results[0]["date"] == "2024-01-10"
