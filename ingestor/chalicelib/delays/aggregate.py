from datetime import datetime
import pandas as pd


def group_monthly_data(df: pd.DataFrame, start_date: str):
    # TODO: Aggregate the sub values
    df_monthly = df.drop(columns=["line"]).groupby(df["line"]).resample("ME").sum()
    df_monthly = df_monthly.fillna(0)
    df_monthly["date"] = [datetime(x[1].year, x[1].month, 1).strftime("%Y-%m-%d") for x in df_monthly.index.tolist()]
    # Drop the first month if it is incomplete
    if datetime.fromisoformat(start_date).day != 1:
        df_monthly = df_monthly.tail(-1)
    return df_monthly


def group_weekly_data(df: pd.DataFrame, start_date: str):
    # Group from Monday - Sunday
    df_weekly = df.resample("W-SUN").agg(
        {
            "total_delay_time": "sum",
            "disabled_vehicle": "sum",
            "signal_problem": "sum",
            "power_problem": "sum",
            "brake_problem": "sum",
            "door_problem": "sum",
            "switch_problem": "sum",
            "track_issue": "sum",
            "track_work": "sum",
            "car_traffic": "sum",
            "mechanical_problem": "sum",
            "police_activity": "sum",
            "medical_emergency": "sum",
            "flooding": "sum",
            "fire": "sum",
            "other": "sum",
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
    # Formats daily data on delays for Dynamo

    df_daily = df.resample("D").agg(
        {
            "total_delay_time": "sum",
            "disabled_vehicle": "sum",
            "signal_problem": "sum",
            "power_problem": "sum",
            "brake_problem": "sum",
            "door_problem": "sum",
            "switch_problem": "sum",
            "track_issue": "sum",
            "track_work": "sum",
            "car_traffic": "sum",
            "mechanical_problem": "sum",
            "police_activity": "sum",
            "medical_emergency": "sum",
            "flooding": "sum",
            "fire": "sum",
            "other": "sum",
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
