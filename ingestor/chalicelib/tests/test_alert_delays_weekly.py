from datetime import date, timedelta
from decimal import Decimal

from .. import constants
from ..delays import process


def _daily_rows(line, start, days):
    # Each day's delay equals its day of month, so a partial week can't match a full one by accident
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        rows.append(
            {
                "line": line,
                "date": d.isoformat(),
                "total_delay_time": Decimal(d.day),
                **{k: Decimal(0) for k in constants.DELAY_BY_TYPE},
                "other": Decimal(d.day),
            }
        )
    return rows


def _run_weekly(monkeypatch, rows, start_date, end_date, lines):
    def fake_query(params, table):
        # Only return rows inside the requested date range, like DynamoDB would
        values = params["KeyConditionExpression"].get_expression()["values"]
        line = values[0].get_expression()["values"][1]
        lo, hi = values[1].get_expression()["values"][1:]
        return [r for r in rows if r["line"] == line and lo <= r["date"] <= hi]

    written = []
    monkeypatch.setattr(process.dynamo, "query_dynamo", fake_query)
    monkeypatch.setattr(process.dynamo, "dynamo_batch_write", lambda items, table: written.extend(items))
    process.update_weekly_from_daily(start_date, end_date, lines)
    return {(r["line"], r["date"]): r for r in written}


def test_weekly_starting_on_sunday_writes_full_weeks(monkeypatch):
    # The Monday cron run starts its 15-day lookback on a Sunday
    first_monday = date(2026, 9, 7)
    rows = _daily_rows("Red", first_monday, 21) + _daily_rows("CR-Lowell", first_monday, 21)
    run_date = date(2026, 9, 28)
    weekly = _run_weekly(monkeypatch, rows, run_date - timedelta(days=15), run_date, ["Red", "CR-Lowell"])

    for week_start in [date(2026, 9, 7), date(2026, 9, 14), date(2026, 9, 21)]:
        full_week = sum(
            r["total_delay_time"]
            for r in rows[:21]
            if week_start.isoformat() <= r["date"] < (week_start + timedelta(days=7)).isoformat()
        )
        assert weekly[("Red", week_start.isoformat())]["total_delay_time"] == full_week
        assert weekly[("Red", week_start.isoformat())]["other"] == full_week
        assert weekly[("CommuterRail", week_start.isoformat())]["total_delay_time"] == full_week
