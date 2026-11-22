"""Build and publish static/landing/overview.json, the home page scoreboard.

Mirrors the dashboard's modules/home/compute/buildHome.ts. Run locally (writes a file, never
uploads):
    cd ingestor && uv run python -m chalicelib.overview --out overview.json [--inputs DIR] [--now ISO]
"""

import argparse
import json
import math
from datetime import date, datetime, timedelta, timezone

from .. import constants, landing, s3
from . import sources
from .cells import (
    baseline_for,
    judged_cell,
    mph,
    na_cell,
    part_of,
    ratio_cell,
    sum_histories,
    trip_metrics_series,
    worst_of,
)
from .config import (
    CR_DROPPED,
    HERO,
    LATE_RIDERSHIP,
    METRIC_ORDER,
    METRICS,
    ROWS,
    SLOW_ZONES,
    UNJUDGED_METRICS,
    WINDOW_WEEKS,
    RowConfig,
)
from .delays import bus_delays_cell, subway_delays_cell
from .delivered import SUBWAY_ROUTES, bus_delivered_share, subway_delivered_share, with_delivered
from .dropped import RECENT_WEEKS, dropped_cell, weekly_totals
from .frequency import midday_headway
from .reliability import cr_reliability_cell, reliability_cell
from .ridership import ridership_cell
from .series import Window, days_between, week_start, from_record, js_round, mean, median, take_window, trend_of
from .status import headline_for

SCHEMA_VERSION = 1
OVERVIEW_KEY = "static/landing/overview.json"

SLOW_ZONE_KEYS = {
    "line-red": "Red",
    "line-orange": "Orange",
    "line-blue": "Blue",
    "line-green": "Green",
    "line-mattapan": "Mattapan",
}

COMING_SOON = "Coming to the home page soon."

# The MBTA publishes no ridership for these lines (service_ridership_dashboard LINE_IDS_WITHOUT_RIDERSHIP).
LINES_WITHOUT_RIDERSHIP = {"line-mattapan"}

# Routes with at least this many dropped trips and under this share waived may include tracking gaps.
MIN_DROPPED_FOR_WAIVER_CHECK = 10
MAX_WAIVED_SHARE = 0.1
SUBWAY_DROPPED_SOON = "Coming soon: trains seen at a central stop against the trips scheduled there."

NOT_MEASURED = {"reliability": "Runs to a timetable, so it isn't judged on headways."}


def line_href(row: RowConfig, page: str) -> str:
    return row["href"] if "/" in row["href"][1:] else f"{row['href']}/{page}"


def slow_zone_window(delay_totals: dict, keys: list[str]) -> Window:
    """Weekly means of daily slow-zone delay (seconds), summed over `keys`, for the last 12 weeks.
    Weeks are 7-day blocks counted from the start of the 84 days, not calendar weeks."""
    # A day missing a key is left out, not read as 0 seconds of delay (a green win), so a line
    # absent from the file comes out as "insufficient".
    days = [
        (day["date"][:10], sum(day[key] for key in keys))
        for day in delay_totals.get("data", [])[-WINDOW_WEEKS * 7 :]
        if all(day.get(key) is not None for key in keys)
    ]
    weeks = [(days[i][0], mean([value for _, value in days[i : i + 7]])) for i in range(0, len(days), 7)]
    return Window(points=weeks, values=[value for _, value in weeks], as_of=days[-1][0] if days else None)


def slow_zone_history(delay_totals: dict, keys: list[str]) -> list[list]:
    """Monthly means of daily slow-zone delay in minutes, summed over `keys`, oldest first."""
    months: dict[str, list[float]] = {}
    for day in delay_totals.get("data", []):
        months.setdefault(day["date"][:7], []).append(sum(day.get(key) or 0 for key in keys) / 60)
    return [[month, round(mean(values), 1)] for month, values in sorted(months.items())]


def subway_cells(row: RowConfig, src: sources.Sources, srd_line: dict | None) -> dict[str, dict]:
    line = row["line"]
    now, baselines = src.now, src.baselines
    trips = src.trip_metrics.get(line)
    cells: dict[str, dict] = {}

    cells["service"] = with_delivered(
        ratio_cell(
            "service",
            trip_metrics_series(trips, lambda d: d.get("count")),
            baseline_for(baselines, "service", [line]),
            now=now,
            aggregate="median",
            unit="pct",
            raw_unit_label="trips/day",
            href=line_href(row, "service"),
        ),
        subway_delivered_share(
            (src.subway_delivered or {}).get(line),
            (src.subway_scheduled or {}).get(line),
            now,
            set(SUBWAY_ROUTES[line][0]) if line in SUBWAY_ROUTES else None,
        ),
        "service",
    )

    cells["dropped"] = na_cell("dropped", "pct", SUBWAY_DROPPED_SOON)

    speed = ratio_cell(
        "speed",
        trip_metrics_series(trips, mph),
        baseline_for(baselines, "speed", [line]),
        now=now,
        aggregate="median",
        unit="mph",
        raw_unit_label="mph",
        headline_raw=True,
        href=line_href(row, "speed"),
    )

    cells["reliability"] = reliability_cell(
        None if src.subway_headways is None else src.subway_headways.get(line, []),
        now=now,
        href=line_href(row, "reliability"),
        notes=["Trunk stops count every branch's trains together."] if line in ("line-red", "line-green") else None,
    )

    cells["delays"] = subway_delays_cell(
        line,
        srd_line,
        src.delays,
        now=now,
        href=line_href(row, "delays"),
    )

    slow_zone_days = slow_zone_window(src.delay_totals, [SLOW_ZONE_KEYS[line]])
    slow_zones = judged_cell(
        "speed",
        mean(slow_zone_days.values) if slow_zone_days.values else None,
        unit="seconds",
        window=slow_zone_days,
        now=now,
        as_of_offset_days=0,
        href=line_href(row, "slowzones"),
        method=SLOW_ZONES,
    )
    cells["speed"] = worst_of(
        speed,
        [
            (part_of("speed", "Speed", speed, "of best"), speed),
            (part_of("slowZones", "Slow zones", slow_zones, "a day"), slow_zones),
        ],
    )

    if srd_line and srd_line.get("ridershipHistory"):
        cells["ridership"] = ridership_cell(
            from_record(srd_line["ridershipHistory"]), now=now, href=line_href(row, "ridership")
        )
    elif line in LINES_WITHOUT_RIDERSHIP:
        cells["ridership"] = na_cell("ridership", "pct", "The MBTA doesn't publish ridership for this line.")
    else:
        cells["ridership"] = {**na_cell("ridership", "pct", "No ridership data."), "status": "insufficient"}
    return cells


def lines_of_kind(srd: dict, kinds: list[str]) -> list[dict]:
    return [line for line in srd["lineData"].values() if line.get("lineKind") in kinds]


def mode_service_cell(row: RowConfig, src: sources.Sources, mode: str, baseline_id: str) -> dict:
    return ratio_cell(
        "service",
        from_record((src.service_ridership["modeData"].get(mode) or {}).get("totalServiceHistory")),
        baseline_for(src.baselines, "scheduledService", [baseline_id]),
        now=src.now,
        aggregate="median",
        unit="pct",
        raw_unit_label="scheduled trips/day",
        href=row["href"],
    )


def frequent_bus_lines(src: sources.Sources) -> list[dict]:
    srd = src.service_ridership
    return [srd["lineData"][line_id] for line_id in src.frequent_bus_line_ids if line_id in srd["lineData"]]


def bus_routes_rows(src: sources.Sources, lines: list[dict]) -> list[dict] | None:
    if src.bus_headways is None:
        return None
    return [r for line in lines for route in line.get("routeIds", []) for r in src.bus_headways.get(route, [])]


def mode_ridership_history(src: sources.Sources, mode: str) -> dict[str, float] | None:
    return (src.service_ridership["modeData"].get(mode) or {}).get("totalRidershipHistory")


def mode_ridership_cell(row: RowConfig, src: sources.Sources, mode: str, method: dict | None = None) -> dict:
    return ridership_cell(from_record(mode_ridership_history(src, mode)), now=src.now, href=row["href"], method=method)


def may_include_tracking_gaps(rows: list[dict]) -> bool:
    """Few of the route's dropped trips have an MBTA waiver, so some may be buses that ran with
    their tracking off. The bus leaderboard sets these routes apart the same way
    (common/utils/busTripCounts.ts mayIncludeTrackingGaps)."""
    dropped = sum(r.get("unobserved_trips") or 0 for r in rows)
    waived = sum(r.get("unobserved_waived_trips") or 0 for r in rows)
    return dropped >= MIN_DROPPED_FOR_WAIVER_CHECK and waived < MAX_WAIVED_SHARE * dropped


def bus_dropped_cell(row: RowConfig, src: sources.Sources, lines: list[dict]) -> dict:
    if src.bus_headways is None:
        return {**na_cell("dropped", "pct", "Couldn't load bus trip counts."), "status": "insufficient"}
    routes = [route for line in lines for route in line.get("routeIds", [])]
    # Judged over the same 4 weeks the cell scores; the same routes are left out a year earlier.
    since = (date.fromisoformat(week_start(src.now.date().isoformat())) - timedelta(weeks=RECENT_WEEKS)).isoformat()
    set_aside = {
        route
        for route in routes
        if may_include_tracking_gaps([r for r in src.bus_headways.get(route, []) if r["date"] >= since])
    }
    counted = [route for route in routes if route not in set_aside]

    def weeks(by_route: dict[str, list[dict]] | None, now: datetime):
        rows = [r for route in counted for r in (by_route or {}).get(route, [])]
        return weekly_totals(rows, "scheduled_trips", "unobserved_trips", now)

    notes = ["A scheduled trip with no bus location data counts as dropped."]
    if set_aside:
        count = len(set_aside)
        notes.append(
            f"Leaves out {count} route{'' if count == 1 else 's'} whose missing trips look like tracking gaps, "
            "as the bus leaderboard does."
        )
    return dropped_cell(
        weeks(src.bus_headways, src.now),
        weeks(src.bus_trips_year_ago, src.now - sources.YEAR),
        now=src.now,
        href="/bus/leaderboard?rank=dropped",
        notes=notes,
    )


def cr_dropped_cell(src: sources.Sources) -> dict:
    if src.cr_reliability is None:
        return {**na_cell("dropped", "pct", "Couldn't load cancellation data."), "status": "insufficient"}
    return dropped_cell(
        weekly_totals(src.cr_reliability, "otpDenominator", "cancelled", src.now),
        weekly_totals(src.cr_reliability_year_ago, "otpDenominator", "cancelled", src.now - sources.YEAR),
        now=src.now,
        method=CR_DROPPED,
        href="/commuter-rail/reliability",
        notes=["Trains the MBTA reports as cancelled, from its published counts."],
    )


def bus_reliability_cell(row: RowConfig, src: sources.Sources, lines: list[dict]) -> dict:
    if src.bus_headways is None:
        return reliability_cell(None, now=src.now, href=row["href"])
    rows_by_line = [
        [r for route in line.get("routeIds", []) for r in src.bus_headways.get(route, [])] for line in lines
    ]
    # Bus rows from before headways were counted carry no headway fields.
    with_data = sum(1 for rows in rows_by_line if any(r.get("headways_75_125") is not None for r in rows))
    return reliability_cell(
        [r for rows in rows_by_line for r in rows],
        now=src.now,
        href=row["href"],
        notes=[
            "Mid-route timepoints only.",
            *([f"{with_data} of {len(lines)} routes have headway data."] if with_data < len(lines) else []),
        ],
    )


def build_cells(row: RowConfig, src: sources.Sources) -> dict[str, dict]:
    now, baselines, srd = src.now, src.baselines, src.service_ridership
    row_id = row["id"]

    if row_id in ("red", "orange", "blue", "green", "mattapan"):
        gtfs_id = constants.RIDERSHIP_KEYS.get(row["line"], row["line"])
        return subway_cells(row, src, srd["lineData"].get(gtfs_id))

    if row_id == "commuter-rail":
        return {
            "service": mode_service_cell(row, src, "regional-rail", "mode-regional-rail"),
            "dropped": cr_dropped_cell(src),
            "reliability": cr_reliability_cell(src.cr_reliability, now=now, href="/commuter-rail/reliability"),
            "delays": na_cell("delays", "pct", "Late trains count against on-time % in Reliability instead."),
            "speed": na_cell("speed", "mph", "Not measured for Commuter Rail yet."),
            "ridership": mode_ridership_cell(row, src, "regional-rail", LATE_RIDERSHIP),
        }

    if row_id == "frequent-bus":
        lines = frequent_bus_lines(src)
        with_service = [
            line for line in lines if baseline_for(baselines, "scheduledService", [line["id"]]).value is not None
        ]
        route_note = (
            [f"{len(with_service)} of {len(lines)} routes have a baseline."] if len(with_service) < len(lines) else []
        )
        return {
            "service": with_delivered(
                ratio_cell(
                    "service",
                    sum_histories([line.get("serviceHistory") for line in with_service]),
                    baseline_for(baselines, "scheduledService", [line["id"] for line in with_service]),
                    now=now,
                    aggregate="median",
                    unit="pct",
                    raw_unit_label="scheduled trips/day",
                    href=row["href"],
                    notes=route_note,
                ),
                bus_delivered_share(bus_routes_rows(src, lines), now),
                "trips",
            ),
            "dropped": bus_dropped_cell(row, src, lines),
            "reliability": bus_reliability_cell(row, src, lines),
            "delays": bus_delays_cell(lines, src.delays, now=now),
            "speed": na_cell("speed", "mph", COMING_SOON),
            "ridership": ridership_cell(
                sum_histories([line.get("ridershipHistory") for line in lines]), now=now, href=row["href"]
            ),
        }

    if row_id == "all-bus":
        lines = lines_of_kind(srd, ["bus", "silver"])
        return {
            "service": with_delivered(
                mode_service_cell(row, src, "bus", "mode-bus"),
                bus_delivered_share(bus_routes_rows(src, lines), now),
                "trips",
            ),
            "dropped": bus_dropped_cell(row, src, lines),
            "reliability": bus_reliability_cell(row, src, lines),
            "delays": bus_delays_cell(lines, src.delays, now=now),
            "speed": na_cell("speed", "mph", COMING_SOON),
            # The service & ridership file counts the Silver Line as rapid transit, not bus.
            "ridership": ridership_cell(
                sum_histories(
                    [
                        mode_ridership_history(src, "bus"),
                        *(line.get("ridershipHistory") for line in lines if line.get("lineKind") == "silver"),
                    ]
                ),
                now=now,
                href=row["href"],
            ),
        }

    if row_id == "ferry":
        return {
            "service": mode_service_cell(row, src, "boat", "mode-boat"),
            "dropped": na_cell("dropped", "pct", "Not measured for ferries."),
            "speed": na_cell("speed", "mph", "Not measured for ferries."),
            "ridership": (
                ridership_cell(
                    [(r["date"], r.get("count")) for r in src.ferry_ridership],
                    now=now,
                    href=row["href"],
                    method=LATE_RIDERSHIP,
                )
                if src.ferry_ridership
                else mode_ridership_cell(row, src, "boat", LATE_RIDERSHIP)
            ),
        }

    return {}


def build_row(row: RowConfig, src: sources.Sources) -> dict:
    built = build_cells(row, src)
    cells = {
        metric: built.get(metric)
        or na_cell(metric, METRICS[metric]["score_unit"], NOT_MEASURED.get(metric, "Not measured."))
        for metric in METRIC_ORDER
    }
    return {
        "id": row["id"],
        "label": row["label"],
        "group": row["group"],
        "line": row["line"],
        "href": row["href"],
        "cells": cells,
        "headline": headline_for(
            [cell for metric, cell in cells.items() if metric not in UNJUDGED_METRICS], row["minMeasured"]
        ),
    }


def hero_current(value: float | None, window: Window, method: dict, now: datetime, as_of_offset_days: float) -> dict:
    """A hero value with its asOf, or null once the data is older than the method's stale_days,
    as judged_cell does, so a source that stops updating isn't shown as last week's value."""
    stale = not window.as_of or days_between(window.as_of, now) - as_of_offset_days > method["stale_days"]
    return {"value": None if stale else value, "asOf": window.as_of}


def build_hero(src: sources.Sources) -> list[dict]:
    srd, baselines = src.service_ridership, src.baselines
    service_window = take_window(from_record(srd["summaryData"].get("totalServiceHistory")))
    service_best = baseline_for(baselines, "scheduledService", ["total"]).value

    slow_zones = slow_zone_window(src.delay_totals, list(SLOW_ZONE_KEYS.values()))

    cr_lines = lines_of_kind(srd, ["regional-rail"])
    cr_saturday = midday_headway(cr_lines, "saturday")

    values = {
        "service": {
            **hero_current(
                median(service_window.values) / service_best if service_best else None,
                service_window,
                METRICS["service"],
                src.now,
                as_of_offset_days=6,  # weekly points are dated by week start
            ),
            "trend": trend_of(service_window),
        },
        "slowZones": {
            **hero_current(
                mean(slow_zones.values[-1:]) / 60 if slow_zones.values else None,
                slow_zones,
                SLOW_ZONES,
                src.now,
                as_of_offset_days=0,
            ),
            "trend": trend_of(slow_zones),
            "history": slow_zone_history(src.delay_totals, list(SLOW_ZONE_KEYS.values())),
        },
        "crHeadway": {"value": midday_headway(cr_lines, "weekday")},
    }
    if cr_saturday is not None:
        values["crHeadway"]["note"] = f"{js_round(cr_saturday)} min on Saturdays"
    return [{**config, **values.get(config["id"], {"value": None})} for config in HERO]


def _without_nan(value):
    """NaN and infinity aren't JSON; the page treats them as missing."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: _without_nan(v) for key, v in value.items()}
    if isinstance(value, list):
        return [_without_nan(v) for v in value]
    return value


def build_overview(src: sources.Sources) -> dict:
    return _without_nan(
        {
            "schemaVersion": SCHEMA_VERSION,
            "generatedAt": src.now.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "rows": [build_row(row, src) for row in ROWS],
            "hero": build_hero(src),
        }
    )


def to_json(overview: dict, **kwargs) -> str:
    return json.dumps(overview, allow_nan=False, **kwargs)


def publish(overview: dict):
    body = to_json(overview, separators=(",", ":")).encode("utf-8")
    for bucket in landing.BUCKETS:
        print(f"Uploading {OVERVIEW_KEY} to {bucket}")
        s3.s3.put_object(
            Bucket=bucket,
            Key=OVERVIEW_KEY,
            Body=body,
            ContentType="application/json",
            CacheControl="public, max-age=3600",
        )
    for distribution in landing.DISTRIBUTIONS:
        s3.clear_cf_cache(distribution, [f"/{OVERVIEW_KEY}"])


def store_overview():
    publish(build_overview(sources.load(datetime.now(timezone.utc))))


def main():
    parser = argparse.ArgumentParser(description="Build the home page scoreboard.")
    parser.add_argument("--out", required=True, help="Path to write overview JSON")
    parser.add_argument("--inputs", help="Read the inputs from this directory instead of S3 and DynamoDB")
    parser.add_argument("--now", type=datetime.fromisoformat, help="Pretend it's this time (ISO, UTC if no offset)")
    args = parser.parse_args()
    now = args.now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    src = sources.load_local(args.inputs, now) if args.inputs else sources.load(now)
    overview = build_overview(src)
    with open(args.out, "w") as f:
        f.write(to_json(overview, indent=2))
    print(f"Wrote {args.out}")
