"""Scheduled frequency against a headway goal. Mirrors the dashboard's
modules/home/compute/frequency.ts."""

from dataclasses import dataclass

from .series import median

# The MBTA service day starts around 3 AM; trips after midnight belong to the day before.
SERVICE_DAY_START_HOUR = 3
DAY_WEIGHTS = {"weekday": 5, "saturday": 1, "sunday": 1}


@dataclass
class GoalCoverage:
    # Weighted service hours meeting the goal.
    met: float = 0
    # Weighted service hours, first trip to last.
    total: float = 0


def goal_coverage(line: dict, goal_minutes: float, regime: str = "current") -> GoalCoverage:
    """Hours from the first to the last trip of each service day. Gaps with no trips count as
    service hours that miss the goal: a line that runs every two hours isn't "on time" in the hour
    between. `tripsPerHour` is one direction, so a 10-minute goal needs 6 trips in the hour."""
    needed = 60 / goal_minutes
    coverage = GoalCoverage()
    for day, weight in DAY_WEIGHTS.items():
        levels = ((line.get("serviceRegimes") or {}).get(regime) or {}).get(day)
        if not levels or levels.get("cancelled") or not levels.get("tripsPerHour"):
            continue
        tph = levels["tripsPerHour"]
        hours = tph[SERVICE_DAY_START_HOUR:] + tph[:SERVICE_DAY_START_HOUR]
        running = [i for i, trips in enumerate(hours) if trips > 0]
        if not running:
            continue
        span = hours[running[0] : running[-1] + 1]
        coverage.total += len(span) * weight
        coverage.met += sum(1 for trips in span if trips >= needed) * weight
    return coverage


def combine_coverage(coverages: list[tuple[GoalCoverage, float | None]]) -> float | None:
    """With weights (riders), each line's share counts in proportion to its weight; without, lines
    count by their service hours."""
    weighted = any(weight is not None for _, weight in coverages)
    met = 0.0
    total = 0.0
    for coverage, weight in coverages:
        if not coverage.total:
            continue
        if weighted:
            met += coverage.met / coverage.total * (weight or 0)
            total += weight or 0
        else:
            met += coverage.met
            total += coverage.total
    return met / total if total else None


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
