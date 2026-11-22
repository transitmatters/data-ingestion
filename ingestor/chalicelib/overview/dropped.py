"""Share of scheduled trips that didn't run, over the last 4 complete weeks, against the same weeks
a year earlier.

Bus counts scheduled trips LAMP never saw a bus on (mbta-performance's DeliveredTripMetricsBus
rows), the estimate behind the bus leaderboard's dropped trips. Commuter Rail uses the MBTA's own
cancellation counts, out of the same trips its on-time share counts (cancelled trains included).
"""

from datetime import date, datetime, timedelta

from .cells import judged_cell, month_year
from .series import Window, js_round, week_start

RECENT_WEEKS = 4

# (week start, scheduled trips, dropped trips)
WeekTotal = tuple[str, float, float]


def weekly_totals(rows: list[dict] | None, scheduled_field: str, dropped_field: str, now: datetime) -> list[WeekTotal]:
    """Totals per Monday-to-Sunday week before the current one, oldest first. The newest week is
    dropped if it's missing a day, so a batch that ends partway through it doesn't count, the same
    rule as Commuter Rail's on-time weeks (reliability._cr_weekly_totals)."""
    this_week = week_start(now.date().isoformat())
    totals: dict[str, list[float]] = {}
    days: dict[str, set[str]] = {}
    for row in rows or []:
        if not row.get(scheduled_field):
            continue
        week = week_start(row["date"])
        if week >= this_week:
            continue
        total = totals.setdefault(week, [0, 0])
        total[0] += row[scheduled_field]
        total[1] += row.get(dropped_field) or 0
        days.setdefault(week, set()).add(row["date"])
    weeks = sorted(totals)
    if weeks and len(days[weeks[-1]]) < 7:
        weeks.pop()
    return [(week, *totals[week]) for week in weeks]


def _share(weeks: list[WeekTotal]) -> float | None:
    scheduled = sum(week[1] for week in weeks)
    return sum(week[2] for week in weeks) / scheduled if scheduled else None


def _year_before(week: str) -> str:
    return (date.fromisoformat(week) - timedelta(weeks=52)).isoformat()


def dropped_cell(
    weeks: list[WeekTotal],
    year_ago: list[WeekTotal],
    *,
    now: datetime,
    method: dict | None = None,
    href: str | None = None,
    notes: list[str] | None = None,
) -> dict:
    """`weeks` and `year_ago` are weekly totals. The trend compares the same weeks 52 weeks back, and
    only when every recent week has its twin, so both sides cover like-for-like weeks."""
    recent = weeks[-RECENT_WEEKS:]
    window = Window(
        points=[(week, dropped / scheduled) for week, scheduled, dropped in recent],
        values=[dropped / scheduled for _, scheduled, dropped in recent],
        as_of=recent[-1][0] if recent else None,
    )
    share = _share(recent)
    if share is None:
        return judged_cell(
            "dropped", None, unit="pct", window=window, now=now, method=method, href=href, notes=["No recent data."]
        )

    wanted = {_year_before(week) for week, _, _ in recent}
    matched = [week for week in year_ago if week[0] in wanted]
    then = _share(matched) if len(matched) == len(recent) else None
    cell = judged_cell(
        "dropped",
        share,
        unit="pct",
        window=window,
        now=now,
        min_points=RECENT_WEEKS,
        method=method,
        href=href,
        notes=notes,
        secondary=(
            f"{js_round(sum(w[2] for w in recent)):,} of {js_round(sum(w[1] for w in recent)):,} "
            f"scheduled trips in {len(recent)} weeks"
        ),
        baseline_label=(f"{month_year(matched[0][0])}: {then * 100:.1f}% dropped" if then is not None else None),
    )
    return {**cell, "trend": share - then if then is not None else None, "trendBasis": "yearAgo"}
