from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

EASTERN_TIME = ZoneInfo("US/Eastern")


def service_date(ts: datetime) -> date:
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=EASTERN_TIME)

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def get_current_service_date() -> date:
    return service_date(datetime.now(EASTERN_TIME))


def format_dateint(dtint: int) -> str:
    """Safely takes a dateint of YYYYMMDD to YYYY-MM-DD."""
    return datetime.strptime(str(dtint), "%Y%m%d").strftime("%Y-%m-%d")
