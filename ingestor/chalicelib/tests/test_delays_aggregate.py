import pandas as pd
import pytest
from datetime import date

from ..delays.aggregate import group_daily_data, group_monthly_data, group_weekly_data

# All delay-type columns required by the aggregation functions
DELAY_COLS = [
    "total_delay_time",
    "disabled_vehicle",
    "signal_problem",
    "power_problem",
    "brake_problem",
    "door_problem",
    "switch_problem",
    "track_issue",
    "track_work",
    "car_traffic",
    "mechanical_problem",
    "police_activity",
    "medical_emergency",
    "flooding",
    "fire",
    "other",
]


def make_delay_df(dates: list[str], line: str, total_delays: list[int] = None) -> pd.DataFrame:
    """Build a single-line delay DataFrame with a DatetimeIndex."""
    n = len(dates)
    if total_delays is None:
        total_delays = [0] * n
    data = {col: [0] * n for col in DELAY_COLS}
    data["total_delay_time"] = total_delays
    data["line"] = [line] * n
    df = pd.DataFrame(data, index=pd.to_datetime(dates))
    return df


# --- group_daily_data ---


def test_group_daily_data_returns_records():
    df = make_delay_df(["2024-01-01", "2024-01-02", "2024-01-03"], "Red", [10, 5, 0])
    records = group_daily_data(df, "2024-01-01")
    assert isinstance(records, list)
    assert len(records) == 3


def test_group_daily_data_correct_dates():
    df = make_delay_df(["2024-01-01", "2024-01-02"], "Orange", [10, 20])
    records = group_daily_data(df, "2024-01-01")
    dates = [r["date"] for r in records]
    assert "2024-01-01" in dates
    assert "2024-01-02" in dates


def test_group_daily_data_preserves_totals():
    df = make_delay_df(["2024-01-15"], "Blue", [42])
    records = group_daily_data(df, "2024-01-15")
    assert len(records) == 1
    assert records[0]["total_delay_time"] == 42


def test_group_daily_data_preserves_line():
    df = make_delay_df(["2024-01-01"], "Green-B", [5])
    records = group_daily_data(df, "2024-01-01")
    assert records[0]["line"] == "Green-B"


def test_group_daily_data_gap_days_filtered_out():
    # Days with no data are filled with 0 by fillna, then filtered out by `line != 0`.
    # So gaps in the source data do not appear in the output.
    df = make_delay_df(["2024-01-01", "2024-01-03"], "Red", [10, 20])
    records = group_daily_data(df, "2024-01-01")
    dates = [r["date"] for r in records]
    assert "2024-01-02" not in dates
    assert "2024-01-01" in dates
    assert "2024-01-03" in dates


# --- group_weekly_data ---


def test_group_weekly_data_returns_records():
    dates = [f"2024-01-{d:02d}" for d in range(1, 8)]  # Jan 1-7 (Mon-Sun)
    df = make_delay_df(dates, "Red", [5] * 7)
    records = group_weekly_data(df, "2024-01-01")
    assert isinstance(records, list)
    assert len(records) >= 1


def test_group_weekly_data_week_starts_on_monday():
    # Jan 1 2024 is a Monday; Jan 7 is Sunday → one full week
    dates = [f"2024-01-{d:02d}" for d in range(1, 8)]
    df = make_delay_df(dates, "Blue", [7] * 7)
    records = group_weekly_data(df, "2024-01-01")
    # The week record's date should be Monday Jan 1 (index shifted by -6 from Sunday)
    assert len(records) == 1
    assert records[0]["date"] == "2024-01-01"


def test_group_weekly_data_sums_totals():
    dates = [f"2024-01-{d:02d}" for d in range(1, 8)]  # 7 days
    df = make_delay_df(dates, "Orange", [10] * 7)
    records = group_weekly_data(df, "2024-01-01")
    assert records[0]["total_delay_time"] == 70  # 10 * 7


def test_group_weekly_data_preserves_line():
    dates = [f"2024-01-{d:02d}" for d in range(1, 8)]
    df = make_delay_df(dates, "Green-E", [1] * 7)
    records = group_weekly_data(df, "2024-01-01")
    assert records[0]["line"] == "Green-E"


# --- group_monthly_data ---


def test_group_monthly_data_returns_dataframe():
    # Full month of January
    dates = pd.date_range("2024-01-01", "2024-01-31", freq="D").strftime("%Y-%m-%d").tolist()
    df = make_delay_df(dates, "Red", [1] * 31)
    result = group_monthly_data(df, "2024-01-01")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["date"].iloc[0] == "2024-01-01"


def test_group_monthly_data_drops_incomplete_first_month():
    # Start mid-January — the partial January month should be dropped
    dates = pd.date_range("2024-01-15", "2024-02-29", freq="D").strftime("%Y-%m-%d").tolist()
    df = make_delay_df(dates, "Blue", [1] * len(dates))
    result = group_monthly_data(df, "2024-01-15")
    # Only February should remain (January is incomplete)
    assert len(result) == 1
    assert result["date"].iloc[0] == "2024-02-01"


def test_group_monthly_data_keeps_full_first_month():
    # Start on the 1st — no month should be dropped
    dates = pd.date_range("2024-01-01", "2024-02-29", freq="D").strftime("%Y-%m-%d").tolist()
    df = make_delay_df(dates, "Orange", [2] * len(dates))
    result = group_monthly_data(df, "2024-01-01")
    assert len(result) == 2
    month_dates = set(result["date"].tolist())
    assert "2024-01-01" in month_dates
    assert "2024-02-01" in month_dates
