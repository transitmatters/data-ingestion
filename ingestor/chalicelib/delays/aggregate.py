from datetime import datetime
import numpy as np
import pandas as pd


def group_monthly_data(df: pd.DataFrame, start_date: str):
    """Resamples daily delay data into monthly aggregates.

    Args:
        df: A DataFrame indexed by datetime with delay type columns.
        start_date: Start date string; drops the first month if incomplete.

    Returns:
        A DataFrame with monthly summed delay data.
    """
    # TODO: Aggregate the sub values
    df_monthly = df.groupby("line").resample("M").sum()
    df_monthly = df_monthly.fillna(0)
    df_monthly["date"] = [datetime(x[1].year, x[1].month, 1).strftime("%Y-%m-%d") for x in df_monthly.index.tolist()]
    # Drop the first month if it is incomplete
    if datetime.fromisoformat(start_date).day != 1:
        df_monthly = df_monthly.tail(-1)
    return df_monthly


def group_weekly_data(df: pd.DataFrame, start_date: str):
    """Resamples daily delay data into weekly (Mon-Sun) aggregates.

    Args:
        df: A DataFrame indexed by datetime with delay type columns.
        start_date: Start date string used for period alignment.

    Returns:
        A list of weekly aggregate dicts with delay breakdowns.
    """
    # Group from Monday - Sunday
    df_weekly = df.resample("W-SUN").agg(
        {
            "total_delay_time": np.sum,
            "disabled_vehicle": np.sum,
            "signal_problem": np.sum,
            "power_problem": np.sum,
            "brake_problem": np.sum,
            "door_problem": np.sum,
            "switch_problem": np.sum,
            "track_issue": np.sum,
            "track_work": np.sum,
            "car_traffic": np.sum,
            "mechanical_problem": np.sum,
            "police_activity": np.sum,
            "medical_emergency": np.sum,
            "flooding": np.sum,
            "fire": np.sum,
            "other": np.sum,
            "line": "min",
        }
    )
    df_weekly = df_weekly.fillna(0)
    # Pandas resample uses the end date of the range as the index. So we subtract 6 days to convert to first date of the range.
    df_weekly.index = df_weekly.index - pd.Timedelta(days=6)

    # Convert date back to string.
    df_weekly["date"] = df_weekly.index.strftime("%Y-%m-%d")
    df_weekly = df_weekly[df_weekly["line"] != 0]
    return df_weekly.to_dict(orient="records")


def group_daily_data(df: pd.DataFrame, start_date: str):
    """Resamples and formats delay data at daily granularity for DynamoDB.

    Args:
        df: A DataFrame indexed by datetime with delay type columns.
        start_date: Start date string (currently unused but kept for consistency).

    Returns:
        A list of daily delay dicts with breakdowns by type.
    """

    df_daily = df.resample("D").agg(
        {
            "total_delay_time": np.sum,
            "disabled_vehicle": np.sum,
            "signal_problem": np.sum,
            "power_problem": np.sum,
            "brake_problem": np.sum,
            "door_problem": np.sum,
            "switch_problem": np.sum,
            "track_issue": np.sum,
            "track_work": np.sum,
            "car_traffic": np.sum,
            "mechanical_problem": np.sum,
            "police_activity": np.sum,
            "medical_emergency": np.sum,
            "flooding": np.sum,
            "fire": np.sum,
            "other": np.sum,
            "line": "min",
        }
    )

    # Fill the empties with 0
    df_daily = df_daily.fillna(0)

    # Convert date back to string.
    df_daily["date"] = df_daily.index.strftime("%Y-%m-%d")

    df_daily = df_daily.reset_index(drop=True)

    df_daily = df_daily[df_daily["line"] != 0]

    # Select only the columns we need for storage
    daily_records = df_daily.to_dict(orient="records")

    return daily_records
