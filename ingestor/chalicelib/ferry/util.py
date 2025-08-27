from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import os

EASTERN_TIME = ZoneInfo("US/Eastern")

# Initialize service date cache variables
_service_date_cache = None
_cache_hour = None


def to_dateint(date: date) -> int:
    """turn date into 20220615 e.g."""
    return int(str(date).replace("-", ""))


def output_dir_path(route_id: str, direction_id: str, stop_id: str, ts: datetime) -> str:
    date = service_date(ts)
    delimiter = "_"
    stop_path = f"{route_id}{delimiter}{direction_id}{delimiter}{stop_id}"
    mode = "ferry"

    return os.path.join(
        f"daily-{mode}-data",
        stop_path,
        f"Year={date.year}",
        f"Month={date.month}",
        f"Day={date.day}",
    )


def service_date(ts: datetime) -> date:
    # In practice a None TZ is UTC, but we want to be explicit
    # In many places we have an implied eastern
    ts = ts.replace(tzinfo=EASTERN_TIME)

    if ts.hour >= 3 and ts.hour <= 23:
        return date(ts.year, ts.month, ts.day)

    prior = ts - timedelta(days=1)
    return date(prior.year, prior.month, prior.day)


def get_current_service_date() -> date | None:
    """
    Get the current service date.
    We cache the service date for the current hour to prevent
    unnecessary timezone conversions, to save CPU.
    """
    global _service_date_cache, _cache_hour
    now = datetime.now(EASTERN_TIME)
    if now.hour != _cache_hour:
        _service_date_cache = service_date(now)
        _cache_hour = now.hour
    return _service_date_cache


def service_date_iso8601(ts: datetime) -> str:
    return service_date(ts).isoformat()
