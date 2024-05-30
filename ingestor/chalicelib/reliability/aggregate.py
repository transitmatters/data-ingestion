from datetime import datetime
import pandas as pd


def group_monthly_data(df: pd.DataFrame, start_date: str):
    # TODO: Aggregate the sub values
    df_monthly = df.groupby("line").resample("M").sum()
    df_monthly = df_monthly.fillna(0)
    df_monthly["date"] = [datetime(x[1].year, x[1].month, 1).strftime("%Y-%m-%d") for x in df_monthly.index.tolist()]
    # Drop the first month if it is incomplete
    if datetime.fromisoformat(start_date).day != 1:
        df_monthly = df_monthly.tail(-1)
    return df_monthly


def group_weekly_data(df: pd.DataFrame, start_date: str):
    # Group from Monday - Sunday
    # TODO: Aggregate the sub values
    df_weekly = df.groupby("line").resample("W-SUN").sum()
    df_weekly = df_weekly.fillna(0)
    # Pandas resample uses the end date of the range as the index. So we subtract 6 days to convert to first date of the range.
    df_weekly.index = df_weekly.index - pd.Timedelta(days=6)
    # Drop the first week if it is incomplete
    if datetime.fromisoformat(start_date).weekday() != 0:
        df_weekly = df_weekly.tail(-1)
    # Convert date back to string.
    df_weekly["date"] = df_weekly.index.strftime("%Y-%m-%d")
    return df_weekly.to_dict(orient="records")
