from datetime import datetime
import pandas as pd

from .. import constants


def group_monthly_data(df: pd.DataFrame, start_date: str):
    # TODO: Aggregate the sub values
    df_monthly = df.groupby("line").resample("M").sum()
    df_monthly = df_monthly.fillna(0)
    df_monthly["date"] = [datetime(x[1].year, x[1].month, 1).strftime("%Y-%m-%d") for x in df_monthly.index.tolist()]
    # Drop the first month if it is incomplete
    if datetime.fromisoformat(start_date).day != 1:
        df_monthly = df_monthly.tail(-1)
    return df_monthly


# What the alerts said ("delays of about 15 minutes"), summed per alert.
STATED_DELAY_COLUMNS = ["total_delay_time", *constants.DELAY_BY_TYPE]
# How long those same alerts were live (see duration.py). Absent for days with no
# LAMP alerts file, which must stay absent rather than read as zero minutes.
ALERT_MINUTES_COLUMNS = ["delay_alert_minutes", *(f"delay_alert_minutes_{t}" for t in constants.DELAY_BY_TYPE)]


def _sum_if_any(series: pd.Series):
    return series.sum(min_count=1)


def _resample(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    agg = {column: "sum" for column in STATED_DELAY_COLUMNS}
    agg.update({column: _sum_if_any for column in ALERT_MINUTES_COLUMNS if column in df.columns})
    agg["line"] = "min"
    resampled = df.resample(rule).agg(agg)
    resampled[STATED_DELAY_COLUMNS] = resampled[STATED_DELAY_COLUMNS].fillna(0)
    resampled["line"] = resampled["line"].fillna(0)
    return resampled


def _records(df: pd.DataFrame) -> list[dict]:
    # Drop missing alert-minute values instead of writing NaN, which DynamoDB rejects
    return [{k: v for k, v in record.items() if not pd.isna(v)} for record in df.to_dict(orient="records")]


def group_weekly_data(df: pd.DataFrame, start_date: str):
    # Group from Monday - Sunday
    df_weekly = _resample(df, "W-SUN")
    # Pandas resample uses the end date of the range as the index. So we subtract 6 days to convert to first date of the range.
    df_weekly.index = df_weekly.index - pd.Timedelta(days=6)

    # Convert date back to string.
    df_weekly["date"] = df_weekly.index.strftime("%Y-%m-%d")
    df_weekly = df_weekly[df_weekly["line"] != 0]
    return _records(df_weekly)


def group_daily_data(df: pd.DataFrame, start_date: str):
    # Formats daily data on delays for Dynamo
    df_daily = _resample(df, "D")

    # Convert date back to string.
    df_daily["date"] = df_daily.index.strftime("%Y-%m-%d")

    df_daily = df_daily.reset_index(drop=True)

    df_daily = df_daily[df_daily["line"] != 0]

    return _records(df_daily)
