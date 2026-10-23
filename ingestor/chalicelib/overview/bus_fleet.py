"""Bus Vehicles: the share of trips on battery-electric or hybrid buses. Mirrors the dashboard's
modules/home/compute/busFleet.ts."""

from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta

from .cells import judged_cell, na_cell
from .config import BUS_FLEET_THRESHOLDS
from .series import js_round, take_window

# Weeks averaged for the score, like the rail fleet cell.
RECENT_WEEKS = 4


def week_start(day: str) -> str:
    d = date.fromisoformat(day[:10])
    return (d - timedelta(days=d.weekday())).isoformat()


@dataclass
class Totals:
    trips: float = 0
    battery: float = 0
    hybrid: float = 0
    age_trips: float = 0
    age_sum: float = 0

    def add_row(self, row: dict):
        trips = row.get("fleet_trips") or 0
        self.trips += trips
        self.battery += trips * (row.get("fleet_mix_battery") or 0) / 100
        self.hybrid += trips * (row.get("fleet_mix_hybrid") or 0) / 100
        if row.get("avg_bus_age") is not None:
            self.age_trips += trips
            self.age_sum += trips * row["avg_bus_age"]

    def add(self, other: "Totals"):
        for f in fields(self):
            setattr(self, f.name, getattr(self, f.name) + getattr(other, f.name))


def bus_fleet_cell(
    rows: list[dict] | None, *, now: datetime, href: str | None = None, notes: list[str] | None = None
) -> dict:
    """Weighted by trips over every daily row given, so several routes' rows pool together. `rows`
    is None when they couldn't be loaded."""
    if rows is None:
        return {**na_cell("fleet", "pct", "Couldn't load bus fleet data."), "status": "insufficient"}

    weeks: dict[str, Totals] = {}
    for row in rows:
        if not row.get("fleet_trips"):
            continue
        weeks.setdefault(week_start(row["date"]), Totals()).add_row(row)
    ordered = sorted(weeks.items())
    window = take_window([(day, (t.battery + t.hybrid) / t.trips) for day, t in ordered], drop_low_weeks=False)

    recent = Totals()
    for _, week in ordered[-RECENT_WEEKS:]:
        recent.add(week)

    def share(part: float) -> float | None:
        return part / recent.trips if recent.trips else None

    def pct(part: float) -> str:
        return f"{js_round((share(part) or 0) * 100)}%"

    cell = judged_cell(
        "fleet",
        share(recent.battery + recent.hybrid),
        unit="pct",
        window=window,
        now=now,
        min_points=RECENT_WEEKS,
        direction="higher",
        thresholds=BUS_FLEET_THRESHOLDS,
        score_unit="pct",
        secondary=f"Battery {pct(recent.battery)} · Hybrid {pct(recent.hybrid)} of trips" if recent.trips else None,
        baseline_label=f"Average bus age: {js_round(recent.age_sum / recent.age_trips)} yr"
        if recent.age_trips
        else None,
        notes=(notes or []) if recent.trips else [*(notes or []), "No recent fleet data."],
        href=href,
    )
    cell.pop("spark", None)
    # A fleet shifts over years, so a 2-week trend says nothing here.
    return {**cell, "trend": None}
