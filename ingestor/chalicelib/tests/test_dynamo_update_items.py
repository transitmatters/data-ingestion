from decimal import Decimal
from unittest import mock

from .. import dynamo


def test_update_items_sets_present_fields_and_removes_missing_owned_fields(monkeypatch):
    table = mock.MagicMock()
    monkeypatch.setattr(dynamo.dynamodb, "Table", lambda name: table)
    item = {"route": "line-blue", "date": "2026-11-03", "line": "line-blue", "count": Decimal(4)}

    dynamo.dynamo_update_items([item], "DeliveredTripMetrics", ("route", "date"), remove_if_missing=("count", "median"))

    kwargs = table.update_item.call_args.kwargs
    assert kwargs["Key"] == {"route": "line-blue", "date": "2026-11-03"}
    assert kwargs["UpdateExpression"] == "SET #a0 = :v0, #a1 = :v1 REMOVE #r0"
    assert kwargs["ExpressionAttributeNames"] == {"#a0": "line", "#a1": "count", "#r0": "median"}
    assert kwargs["ExpressionAttributeValues"] == {":v0": "line-blue", ":v1": Decimal(4)}


def test_update_items_skips_empty_list(monkeypatch):
    table_factory = mock.MagicMock()
    monkeypatch.setattr(dynamo.dynamodb, "Table", table_factory)
    dynamo.dynamo_update_items([], "DeliveredTripMetrics", ("route", "date"))
    table_factory.assert_not_called()
