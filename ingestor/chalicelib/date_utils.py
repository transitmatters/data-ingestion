from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

EASTERN_TIME = ZoneInfo("US/Eastern")


def service_date(ts: datetime) -> date:
    """Determines the MBTA service date for a given timestamp.

    Service dates run from 3 AM to 3 AM Eastern. Timestamps between
    midnight and 3 AM are attributed to the previous calendar day.

    Args:
        ts: A datetime to convert. Naive datetimes are read as Eastern time;
            aware ones are converted to Eastern first.

    Returns:
        The service date as a date object.
    """
    # Naive times are read as Eastern wall-clock time. Aware ones are converted, so a UTC
    # timestamp lands on the right day instead of being relabeled as Eastern.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=EASTERN_TIME)
    else:
        ts = ts.astimezone(EASTERN_TIME)

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def get_current_service_date() -> date:
    """Returns the current MBTA service date in Eastern time.

    Returns:
        Today's service date, accounting for the 3 AM rollover.
    """
    return service_date(datetime.now(EASTERN_TIME))


def format_dateint(dtint: int) -> str:
    """Converts an integer date from YYYYMMDD format to YYYY-MM-DD string.

    Args:
        dtint: A date as an integer in YYYYMMDD format (e.g. 20240115).

    Returns:
        The date formatted as a "YYYY-MM-DD" string.
    """
    return datetime.strptime(str(dtint), "%Y%m%d").strftime("%Y-%m-%d")
