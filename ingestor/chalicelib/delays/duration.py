"""How long delay alerts were actually live, from the LAMP alert history.

The text-parsed `total_delay_time` is what the alert *said* ("delays of about 15
minutes"); this measures how long the same alerts were up, so the two can be
compared. It reads mbta-performance's `Alerts/lamp/{date}.json.gz` day files
straight from S3 rather than the dashboard API, because the API only passes
through `active_period.end` -- for subway delays that's a ~2h placeholder set when
the alert is posted, while `closed_timestamp` is when MBTA actually took it down.

Subway alerts stay up a bit longer than the delay they state (a "15 minute" alert
is up ~35 min at the median). Commuter Rail is left out: its alerts are per train
("operating 10-20 minutes behind schedule") and stay up until that train finishes
its trip, so the stated minutes already are the real delay.
"""

import json
from datetime import date, datetime, timedelta
from typing import Optional

from botocore.exceptions import ClientError

from .. import s3
from ..date_utils import EASTERN_TIME

BUCKET = "tm-mbta-performance"
LAMP_KEY = "Alerts/lamp/{date}.json.gz"

# Same effects the dashboard's /api/alerts returns, so this covers the same alerts
# the text-parsed total does.
DASHBOARD_EFFECTS = {"DETOUR", "DELAY", "STOP_CLOSURE", "SERVICE_CHANGE"}

# A delay alert left up for hours was almost always forgotten rather than a real
# multi-hour delay: 90% of subway delay alerts are down within 2h, while a few stay
# open for months.
MAX_ALERT_DURATION = timedelta(hours=3)


def load_lamp_alerts(day: date) -> Optional[dict]:
    """The LAMP alerts day file, or None if it isn't published (e.g. today)."""
    try:
        return json.loads(s3.download(BUCKET, LAMP_KEY.format(date=day.isoformat()), compressed=True))
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
        return None


def _parse(ts: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(ts) if ts else None


def service_day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, 3, tzinfo=EASTERN_TIME)
    return start, datetime.combine(day + timedelta(days=1), start.timetz())


def live_intervals(alerts: dict, line: str, day: date, is_delay) -> list[tuple[str, datetime, datetime]]:
    """(header, start, end) for every delay alert on `line`, clipped to `day`'s service day.

    An alert is live from when it was both posted and in its active period until
    the earlier of its period end and MBTA closing it. Alerts with neither an end
    nor a close time are skipped rather than guessed at.
    """
    day_start, day_end = service_day_bounds(day)
    intervals = []
    for alert in alerts.values():
        attrs = alert["attributes"]
        header = attrs.get("header") or attrs.get("short_header")
        if attrs.get("effect") not in DASHBOARD_EFFECTS or not header or not is_delay({"text": header}):
            continue
        if line not in {entity.get("route") for entity in attrs.get("informed_entity", [])}:
            continue
        created = _parse(attrs.get("created_at"))
        closed = _parse(attrs.get("closed_timestamp"))
        for period in attrs.get("active_period", []):
            start = max(filter(None, [_parse(period["start"]), created]))
            ends = [t for t in (_parse(period.get("end")), closed) if t]
            if not ends:
                continue
            end = min(min(ends), start + MAX_ALERT_DURATION)
            start, end = max(start, day_start), min(end, day_end)
            if end > start:
                intervals.append((header, start, end))
    return intervals


def union_minutes(intervals: list[tuple[str, datetime, datetime]]) -> int:
    """Minutes covered by at least one interval, so overlapping alerts (e.g. several
    late Commuter Rail trains at once) aren't double counted."""
    total = timedelta()
    current_start = current_end = None
    for _, start, end in sorted(intervals, key=lambda interval: interval[1]):
        if current_end is None or start > current_end:
            if current_end is not None:
                total += current_end - current_start
            current_start, current_end = start, end
        else:
            current_end = max(current_end, end)
    if current_end is not None:
        total += current_end - current_start
    return round(total.total_seconds() / 60)
