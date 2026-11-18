from unittest import mock

import numpy as np
import pandas as pd
import pytest

from .. import agg_speed_tables
from ..agg_speed_tables import fleet_cols, group_data_by_date_and_branch, group_monthly_data, group_weekly_data


def _row(route, date, miles, **fleet):
    return {
        "route": route,
        "date": date,
        "line": "line-green",
        "miles_covered": miles,
        "total_time": 1.0,
        "count": 1.0,
        **fleet,
    }


def test_fleet_cols():
    assert fleet_cols(["count", "avg_car_age", "pct_new_trips", "fleet_mix_type7", "line"]) == [
        "avg_car_age",
        "pct_new_trips",
        "fleet_mix_type7",
    ]


def test_weekly_fleet_mix_mean_and_missing_days():
    mix_a = {"fleet_mix_type7": 60.0, "fleet_mix_type9": 40.0}
    mix_b = {"fleet_mix_type7": 40.0, "fleet_mix_type9": 60.0}
    rows = [
        # Week of Mon 2026-09-07: two days with mix data (copied onto each branch row)
        _row("line-green-b", "2026-09-07", 1.0, **mix_a),
        _row("line-green-c", "2026-09-07", 1.0, **mix_a),
        _row("line-green-b", "2026-09-08", 1.0, **mix_b),
        _row("line-green-c", "2026-09-08", 1.0, **mix_b),
        # Week of Mon 2026-09-14: no fleet data at all
        _row("line-green-b", "2026-09-14", 1.0),
        _row("line-green-c", "2026-09-14", 1.0),
    ]
    df = group_data_by_date_and_branch(pd.DataFrame(rows))
    weeks = group_weekly_data(df, "2026-09-07")

    assert weeks[0]["fleet_mix_type7"] == 50.0
    assert weeks[0]["fleet_mix_type9"] == 50.0
    assert np.isclose(weeks[0]["fleet_mix_type7"] + weeks[0]["fleet_mix_type9"], 100)
    # No data means the attribute is omitted, not zero-filled
    assert "fleet_mix_type7" not in weeks[1]
    assert weeks[1]["miles_covered"] == 2.0


def test_monthly_sums_each_month_and_drops_partial_first_month():
    rows = [
        # Starts mid-September, so September is incomplete and dropped
        _row("line-green-b", "2026-09-15", 1.0),
        _row("line-green-c", "2026-09-15", 2.0),
        _row("line-green-b", "2026-10-01", 3.0),
        _row("line-green-c", "2026-10-01", 4.0),
        _row("line-green-b", "2026-10-31", 5.0),
        _row("line-green-c", "2026-10-31", 6.0),
        _row("line-green-b", "2026-11-02", 7.0),
        _row("line-green-c", "2026-11-02", 8.0),
    ]
    df = group_data_by_date_and_branch(pd.DataFrame(rows))
    months = group_monthly_data(df, "2026-09-15")

    assert [m["date"] for m in months] == ["2026-10-01", "2026-11-01"]
    assert [m["miles_covered"] for m in months] == [18.0, 15.0]
    assert [m["line"] for m in months] == ["line-green", "line-green"]


def test_empty_week_is_dropped_not_written_with_line_zero():
    """A week with no daily rows must not be zero-filled into line: 0, the string hash key (#268)."""
    rows = [
        _row("line-green-b", "2026-09-07", 1.0),
        _row("line-green-c", "2026-09-07", 1.0),
        # No rows for the week of 2026-09-14
        _row("line-green-b", "2026-09-21", 2.0),
        _row("line-green-c", "2026-09-21", 2.0),
    ]
    df = group_data_by_date_and_branch(pd.DataFrame(rows))
    weeks = group_weekly_data(df, "2026-09-07")
    assert [w["date"] for w in weeks] == ["2026-09-07", "2026-09-21"]
    assert all(w["line"] == "line-green" for w in weeks)


def test_empty_month_is_dropped():
    rows = [
        _row("line-green-b", "2026-08-03", 1.0),
        _row("line-green-c", "2026-08-03", 1.0),
        _row("line-green-b", "2026-10-05", 2.0),
        _row("line-green-c", "2026-10-05", 2.0),
    ]
    df = group_data_by_date_and_branch(pd.DataFrame(rows))
    months = group_monthly_data(df, "2026-08-01")
    assert [m["date"] for m in months] == ["2026-08-01", "2026-10-01"]


@pytest.mark.parametrize(
    "start, range, expected",
    [
        ("2018-12-01", "weekly", "2018-11-26"),  # backfill_fleet's documented start, a Saturday
        ("2018-11-26", "weekly", "2018-11-26"),
        ("2018-12-15", "monthly", "2018-12-01"),
        ("2018-12-01", "monthly", "2018-12-01"),
    ],
)
def test_period_start(start, range, expected):
    assert agg_speed_tables.period_start(start, range) == expected


def test_populate_table_starts_at_the_period_holding_start_date(monkeypatch):
    seen = {}
    monkeypatch.setattr(agg_speed_tables, "actual_trips_by_line", lambda params: seen.update(params) or [])
    monkeypatch.setattr(agg_speed_tables.dynamo, "dynamo_batch_write", lambda items, table: None)
    agg_speed_tables.populate_table("line-green", "weekly", "2018-12-01")
    assert seen["start_date"] == "2018-11-26"


def test_query_daily_trips_on_route_follows_pagination(monkeypatch):
    table = mock.MagicMock()
    table.query.side_effect = [
        {"Items": [{"route": "line-red-a", "date": "2026-01-01"}], "LastEvaluatedKey": {"k": 1}},
        {"Items": [{"route": "line-red-a", "date": "2026-01-02"}]},
    ]
    monkeypatch.setattr(agg_speed_tables.dynamodb, "Table", lambda name: table)

    items = agg_speed_tables.query_daily_trips_on_route(
        "DeliveredTripMetrics", "line-red-a", "2026-01-01", "2026-01-02"
    )

    assert [i["date"] for i in items] == ["2026-01-01", "2026-01-02"]
    assert table.query.call_args_list[1].kwargs["ExclusiveStartKey"] == {"k": 1}
