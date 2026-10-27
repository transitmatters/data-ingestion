"""Build and publish static/landing/overview.json, the home page scoreboard.

Mirrors the dashboard's modules/home/compute/buildHome.ts. Run locally (writes a file, never
uploads):
    cd ingestor && uv run python -m chalicelib.overview --out overview.json [--inputs DIR] [--now ISO]
"""

import argparse
import json
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .. import constants, landing, s3
from . import sources
from .bus_fleet import bus_fleet_cell
from .cells import (
    baseline_for,
    frequency_cell,
    judged_cell,
    mph,
    na_cell,
    ratio_cell,
    sum_histories,
    trip_metrics_series,
)
from .config import (
    CR_ELECTRIFIED_SHARE,
    CR_ELECTRIFIED_THRESHOLDS,
    FLEET_TYPES,
    FREQUENCY_GOALS,
    FREQUENT_BUS_LINE_IDS,
    HERO,
    IN_SERVICE_SHARE,
    METRIC_ORDER,
    METRICS,
    ROWS,
    SINGLE_TYPE_FLEETS,
    SUBWAY_FREQUENCY_LINE_IDS,
    WINDOW_WEEKS,
    RowConfig,
)
from .frequency import combine_coverage, goal_coverage, midday_headway
from .series import Window, from_record, is_finite, js_round, mean, median, take_window, trend_of
from .status import headline_for

SCHEMA_VERSION = 1
OVERVIEW_KEY = "static/landing/overview.json"
EASTERN = ZoneInfo("America/New_York")

SLOW_ZONE_KEYS = {
    "line-red": "Red",
    "line-orange": "Orange",
    "line-blue": "Blue",
    "line-green": "Green",
    "line-mattapan": "Mattapan",
}

COMING_SOON = "Coming to the home page soon."

NOT_MEASURED = {"slowZones": "Slow zones are tracked on the subway only."}


def line_href(row: RowConfig, page: str) -> str:
    return row["href"] if "/" in row["href"][1:] else f"{row['href']}/{page}"


def slow_zone_window(delay_totals: dict, keys: list[str]) -> Window:
    """Weekly means of daily slow-zone delay (seconds), summed over `keys`, for the last 12 weeks.
    Weeks are 7-day blocks counted from the start of the 84 days, not calendar weeks."""
    days = [
        (day["date"][:10], sum(day.get(key) or 0 for key in keys))
        for day in delay_totals.get("data", [])[-WINDOW_WEEKS * 7 :]
    ]
    weeks = [(days[i][0], mean([value for _, value in days[i : i + 7]])) for i in range(0, len(days), 7)]
    return Window(points=weeks, values=[value for _, value in weeks], as_of=days[-1][0] if days else None)


def _built_from(fleet_type: dict) -> int | None:
    digits = "".join(c for c in (fleet_type.get("years") or "")[:4] if c.isdigit())
    return int(digits) if digits else None


def oldest_in_service(line: str, trips: list[dict] | None) -> dict | None:
    """The oldest car type still carrying riders: at least IN_SERVICE_SHARE percent of cars over
    the last 4 weeks with fleet data. Lines with one car type have no mix, so that type is the
    oldest."""
    single = SINGLE_TYPE_FLEETS.get(line)
    if single:
        return {"type": single, "share": None, "built_from": _built_from(single)}
    types = FLEET_TYPES.get(line)
    if not types:
        return None
    with_mix = [row for row in trips or [] if any(row.get(f"fleet_mix_{t['key']}") is not None for t in types)]
    recent = with_mix[-4:]
    if not recent:
        return None
    for fleet_type in types:  # oldest to newest
        share = mean([row.get(f"fleet_mix_{fleet_type['key']}") or 0 for row in recent])
        if share >= IN_SERVICE_SHARE:
            return {"type": fleet_type, "share": share, "built_from": _built_from(fleet_type)}
    return None


def subway_cells(row: RowConfig, src: sources.Sources, frequency_lines: list[dict]) -> dict[str, dict]:
    line = row["line"]
    now, baselines = src.now, src.baselines
    trips = src.trip_metrics.get(line)
    cells: dict[str, dict] = {}

    cells["service"] = ratio_cell(
        "service",
        trip_metrics_series(trips, lambda d: d.get("count")),
        baseline_for(baselines, "service", [line]),
        now=now,
        aggregate="median",
        unit="pct",
        raw_unit_label="trips/day",
        href=line_href(row, "service"),
    )

    if frequency_lines:
        cells["frequency"] = frequency_cell(
            [(frequency_line, None) for frequency_line in frequency_lines],
            FREQUENCY_GOALS["subway"],
            now=now,
            as_of=src.service_ridership["summaryData"]["endDate"],
            href=line_href(row, "service"),
            notes=["Trips on the shared trunk, all branches together."] if line == "line-green" else None,
        )
    else:
        cells["frequency"] = {**na_cell("frequency", "pct", "Not in the schedule data yet."), "status": "insufficient"}

    cells["speed"] = ratio_cell(
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

    slow_zones = slow_zone_window(src.delay_totals, [SLOW_ZONE_KEYS[line]])
    cells["slowZones"] = judged_cell(
        "slowZones",
        mean(slow_zones.values) if slow_zones.values else None,
        unit="seconds",
        window=slow_zones,
        now=now,
        as_of_offset_days=0,
        href=line_href(row, "slowzones"),
    )

    age_window = take_window(trip_metrics_series(trips, lambda d: d.get("avg_car_age")), drop_low_weeks=False)
    recent_age = mean(age_window.values[-4:])
    new_trips = mean([row["pct_new_trips"] for row in (trips or [])[-4:] if is_finite(row.get("pct_new_trips"))])
    oldest = oldest_in_service(line, trips)
    oldest_age = (
        now.astimezone(EASTERN).year - oldest["built_from"] if oldest and oldest["built_from"] is not None else None
    )
    context = [
        f"Average car age: {js_round(recent_age)} yr" if is_finite(recent_age) else None,
        f"{js_round(new_trips)}% of trips on new trains" if is_finite(new_trips) and new_trips > 0 else None,
    ]
    context = [part for part in context if part]
    secondary = None
    if oldest:
        secondary = f"Oldest in service: {oldest['type']['label']} ({oldest['type']['years']})"
        if oldest["share"] is not None:
            secondary += f", {js_round(oldest['share'])}% of cars"
    fleet = judged_cell(
        "fleet",
        oldest_age,
        unit="years",
        window=age_window,
        now=now,
        secondary=secondary,
        baseline_label=" · ".join(context) if context else None,
        # Blue and Mattapan have no Fleet tab (one car type each), so their fleet links go nowhere new.
        href=row["href"] if line in ("line-blue", "line-mattapan") else line_href(row, "fleet"),
    )
    fleet.pop("spark", None)
    # A car type's age only moves once a year, so a 2-week trend says nothing here.
    cells["fleet"] = {**fleet, "trend": None}
    return cells


def lines_of_kind(srd: dict, kinds: list[str]) -> list[dict]:
    return [line for line in srd["lineData"].values() if line.get("lineKind") in kinds]


def recent_riders(line: dict) -> float:
    values = [value for _, value in from_record(line.get("ridershipHistory"))[-WINDOW_WEEKS:]]
    return mean(values) if values else 0


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


def frequent_bus_lines(srd: dict) -> list[dict]:
    return [srd["lineData"][line_id] for line_id in FREQUENT_BUS_LINE_IDS if line_id in srd["lineData"]]


def frequent_bus_fleet_cell(row: RowConfig, src: sources.Sources, lines: list[dict]) -> dict:
    if src.bus_fleet is None:
        return bus_fleet_cell(None, now=src.now, href=row["href"])
    rows_by_line = [[r for route in line.get("routeIds", []) for r in src.bus_fleet.get(route, [])] for line in lines]
    with_data = sum(1 for rows in rows_by_line if any(r.get("fleet_trips") for r in rows))
    return bus_fleet_cell(
        [r for rows in rows_by_line for r in rows],
        now=src.now,
        href=row["href"],
        notes=[f"{with_data} of {len(lines)} routes have fleet data."] if with_data < len(lines) else None,
    )


def build_cells(row: RowConfig, src: sources.Sources) -> dict[str, dict]:
    now, baselines, srd = src.now, src.baselines, src.service_ridership
    as_of = srd["summaryData"]["endDate"]
    row_id = row["id"]

    if row_id in ("red", "orange", "blue", "green", "mattapan"):
        gtfs_id = constants.RIDERSHIP_KEYS.get(row["line"], row["line"])
        return subway_cells(row, src, [srd["lineData"][gtfs_id]] if gtfs_id in srd["lineData"] else [])

    if row_id == "commuter-rail":
        lines = lines_of_kind(srd, ["regional-rail"])
        return {
            "service": mode_service_cell(row, src, "regional-rail", "mode-regional-rail"),
            "frequency": frequency_cell(
                [(line, None) for line in lines],
                FREQUENCY_GOALS["commuterRail"],
                now=now,
                as_of=as_of,
                href=row["href"],
            ),
            "speed": na_cell("speed", "mph", "Not measured for Commuter Rail yet."),
            "fleet": judged_cell(
                "fleet",
                CR_ELECTRIFIED_SHARE,
                unit="pct",
                window=Window(values=[1], as_of=as_of),
                now=now,
                min_points=1,
                as_of_offset_days=0,
                direction="higher",
                thresholds=CR_ELECTRIFIED_THRESHOLDS,
                score_unit="pct",
                secondary="Share of service run electric",
            ),
        }

    if row_id == "frequent-bus":
        lines = frequent_bus_lines(srd)
        with_service = [
            line for line in lines if baseline_for(baselines, "scheduledService", [line["id"]]).value is not None
        ]
        route_note = (
            [f"{len(with_service)} of {len(lines)} routes have a baseline."] if len(with_service) < len(lines) else []
        )
        return {
            "service": ratio_cell(
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
            "frequency": frequency_cell(
                [(line, None) for line in lines],
                FREQUENCY_GOALS["frequentBus"],
                now=now,
                as_of=as_of,
                href=row["href"],
                notes=["Silver Line branches that share a street count together."],
            ),
            "speed": na_cell("speed", "mph", COMING_SOON),
            "fleet": frequent_bus_fleet_cell(row, src, lines),
        }

    if row_id == "all-bus":
        lines = lines_of_kind(srd, ["bus", "silver"])
        return {
            "service": mode_service_cell(row, src, "bus", "mode-bus"),
            "frequency": frequency_cell(
                [(line, recent_riders(line)) for line in lines],
                FREQUENCY_GOALS["bus"],
                now=now,
                as_of=as_of,
                href=row["href"],
            ),
            "speed": na_cell("speed", "mph", COMING_SOON),
            "fleet": bus_fleet_cell(
                None if src.bus_fleet is None else src.bus_fleet.get(sources.SYSTEM_WIDE_ROUTE, []),
                now=now,
                href=row["href"],
            ),
        }

    if row_id == "ferry":
        return {
            "service": mode_service_cell(row, src, "boat", "mode-boat"),
            "frequency": na_cell("frequency", "pct", "No frequency goal for ferries yet."),
            "speed": na_cell("speed", "mph", "Not measured for ferries."),
            "fleet": na_cell("fleet", "years", "Not measured for ferries."),
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
        "headline": headline_for(list(cells.values()), row["minMeasured"]),
    }


def build_hero(src: sources.Sources) -> list[dict]:
    srd, baselines = src.service_ridership, src.baselines
    subway_lines = [srd["lineData"][i] for i in SUBWAY_FREQUENCY_LINE_IDS if i in srd["lineData"]]
    subway = combine_coverage([(goal_coverage(line, FREQUENCY_GOALS["subway"]), None) for line in subway_lines])

    service_window = take_window(from_record(srd["summaryData"].get("totalServiceHistory")))
    service_best = baseline_for(baselines, "scheduledService", ["total"]).value

    slow_zones = slow_zone_window(src.delay_totals, list(SLOW_ZONE_KEYS.values()))

    frequent = combine_coverage(
        [(goal_coverage(line, FREQUENCY_GOALS["frequentBus"]), None) for line in frequent_bus_lines(srd)]
    )

    cr_lines = lines_of_kind(srd, ["regional-rail"])
    cr_saturday = midday_headway(cr_lines, "saturday")

    values = {
        "subwayFrequency": {"value": subway},
        "service": {
            "value": median(service_window.values) / service_best if service_best else None,
            "trend": trend_of(service_window),
        },
        "slowZones": {
            "value": mean(slow_zones.values[-1:]) / 60 if slow_zones.values else None,
            "trend": trend_of(slow_zones),
        },
        "frequentBus": {"value": frequent},
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
