"""Scheduled trips by hour, from the service & ridership dashboard's service regimes."""

from .series import median

# The MBTA service day starts around 3 AM; trips after midnight belong to the day before.
SERVICE_DAY_START_HOUR = 3


def _service_day_hours(line: dict, day: str) -> list[float]:
    """Trips per hour from the first trip of `day` to the last, or [] if it doesn't run."""
    levels = ((line.get("serviceRegimes") or {}).get("current") or {}).get(day)
    if not levels or levels.get("cancelled") or not levels.get("tripsPerHour"):
        return []
    tph = levels["tripsPerHour"]
    hours = tph[SERVICE_DAY_START_HOUR:] + tph[:SERVICE_DAY_START_HOUR]
    running = [i for i, trips in enumerate(hours) if trips > 0]
    return hours[running[0] : running[-1] + 1] if running else []


def service_span_hours(line: dict, day: str) -> int:
    """Hours from the first to the last trip on a weekday, saturday or sunday."""
    return len(_service_day_hours(line, day))


def midday_headway(lines: list[dict], day: str) -> float | None:
    """Median midday (10 AM–3 PM) headway in minutes across `lines` for a service day."""
    headways = []
    for line in lines:
        tph = (((line.get("serviceRegimes") or {}).get("current") or {}).get(day) or {}).get("tripsPerHour")
        if not tph:
            continue
        midday = tph[10:15]
        if not midday:
            continue
        avg = sum(midday) / len(midday)
        if avg > 0:
            headways.append(60 / avg)
    return median(headways) if headways else None
