import math
from datetime import date, datetime, timedelta, timezone

import pytest

from ..overview import build, sources
from ..overview.bus_fleet import bus_fleet_cell
from ..overview.cells import baseline_for, format_number, part_of, worst_of
from ..overview.frequency import combine_coverage, goal_coverage
from ..overview.reliability import cr_reliability_cell, reliability_cell
from ..overview.series import take_window, trend_of
from ..overview.status import headline_for, status_for

NOW = datetime(2026, 10, 11, 12, tzinfo=timezone.utc)


def daily(start: str, days: int, **fields) -> list[dict]:
    first = date.fromisoformat(start)
    return [{"date": (first + timedelta(days=i)).isoformat(), "route": "all", **fields} for i in range(days)]


def weekly(start: str, values: list[float | None]) -> list[tuple[str, float | None]]:
    first = date.fromisoformat(start)
    return [((first + timedelta(weeks=i)).isoformat(), value) for i, value in enumerate(values)]


def baselines(by_id: dict) -> dict:
    return {
        "schemaVersion": 1,
        "lookbackYears": 3,
        "minHistoryWeeks": 52,
        "heldBackReasons": {
            "notComparable": "Counts branch trips differently from the dashboard's service metric.",
            "newReason": "Something we have no local copy for.",
        },
        "metrics": {"scheduledService": {"series": by_id}},
    }


def note_for(series: dict) -> str:
    return baseline_for(
        baselines({"id": {"value": None, "bestWindow": None, **series}}), "scheduledService", ["id"]
    ).note


def test_baseline_sums_the_series_and_labels_a_single_one():
    file = baselines(
        {
            "a": {"value": 100, "bestWindow": {"start": "2019-06-10", "end": "2019-09-01", "weeksWithData": 12}},
            "b": {"value": 50, "bestWindow": None},
        }
    )
    both = baseline_for(file, "scheduledService", ["a", "b"])
    assert (both.value, both.label) == (150, None)
    assert baseline_for(file, "scheduledService", ["a"]).label == "Jun 2019"


def test_baseline_notes_explain_why_there_is_none():
    assert note_for({"heldBack": "notComparable"}) == "No baseline yet: service was counted differently before."
    assert note_for({"heldBack": "newReason"}) == "No baseline yet. Something we have no local copy for."
    assert (
        note_for(
            {
                "insufficientHistory": True,
                "historyWindow": {"start": "2025-05-26", "end": "2023-10-05"},
                "historyWeeks": 0,
            }
        )
        == "No baseline yet: records start May 2025, and baselines only use weeks at least 3 years old."
    )
    assert (
        note_for(
            {
                "insufficientHistory": True,
                "historyWindow": {"start": "2023-01-02", "end": "2023-10-05"},
                "historyWeeks": 40,
            }
        )
        == "No baseline yet: 40 weeks of records are at least 3 years old, and a baseline needs 52."
    )
    assert baseline_for(None, "scheduledService", ["id"]).note == "No baseline yet."


def test_format_number_matches_the_dashboard():
    assert format_number(123456.5) == "123,457"
    assert format_number(150.5) == "151"
    assert format_number(12.25) == "12.3"
    assert format_number(0.25) == "0.3"


def test_take_window_drops_up_to_three_shutdown_weeks():
    series = weekly("2026-01-05", [100, 100, 10, 100, None, 20, 100, 100, 30, 40, 100, 100, 100])
    window = take_window(series)
    assert len(window.points) == 12  # the first week falls outside
    assert window.excluded == 3
    assert 40 in window.values and 10 not in window.values
    assert window.as_of == "2026-03-30"
    assert take_window(series, drop_low_weeks=False).excluded == 0


def test_trend_compares_the_last_two_weeks_with_the_window():
    assert trend_of(take_window(weekly("2026-01-05", [100, 100, 100, 100, 110, 110]))) == pytest.approx(
        110 / (620 / 6) - 1
    )
    assert trend_of(take_window(weekly("2026-01-05", [100, 100, 100]))) is None


def test_status_for_runs_both_ways():
    assert [status_for(s, "higher", (1, 0.9, 0.75)) for s in (1.1, 0.95, 0.8, 0.5)] == [
        "win",
        "good",
        "room",
        "problem",
    ]
    assert [status_for(s, "lower", (30, 60, 180)) for s in (10, 45, 120, 500)] == ["win", "good", "room", "problem"]
    assert status_for(None, "higher", (1, 0.9, 0.75)) == "insufficient"
    assert status_for(math.nan, "higher", (1, 0.9, 0.75)) == "insufficient"


def cell(metric: str, status: str, score: float | None = None) -> dict:
    return {"metric": metric, "status": status, "score": score}


def test_headline_is_worst_of_and_lists_the_worst_first():
    cells = [
        cell("service", "room", 0.85),
        cell("speed", "problem", 0.5),
        cell("frequency", "problem", 0.7),
        cell("fleet", "na"),
    ]
    assert headline_for(cells, 2) == {"kind": "needsWork", "heldBackBy": ["speed", "frequency", "service"]}
    assert headline_for(cells, 4)["kind"] == "limited"
    wins = [cell("service", "win", 1), cell("speed", "good", 0.96)]
    assert headline_for(wins, 2)["kind"] == "strong"


def judged(status: str, score: float | None, direction: str, thresholds, notes: list[str] | None = None) -> dict:
    return {
        "metric": "speed",
        "status": status,
        "score": score,
        "value": score,
        "unit": "pct",
        "trend": None,
        "direction": direction,
        "thresholds": list(thresholds),
        "notes": notes,
    }


def speed_and_slow_zones(speed: dict, slow_zones: dict) -> dict:
    return worst_of(speed, [(part_of("Speed", speed), speed), (part_of("Slow zones", slow_zones), slow_zones)])


def test_worst_of_takes_the_worse_part():
    speed = judged("good", 0.96, "higher", (1, 0.95, 0.88))
    slow = judged("problem", 240, "lower", (30, 60, 180), notes=["Latest data is from Oct 3."])
    merged = speed_and_slow_zones(speed, slow)
    assert merged["status"] == "problem"
    assert merged["score"] == 0.96  # the headline stays the speed
    assert [(p["label"], p["status"], p["score"]) for p in merged["parts"]] == [
        ("Speed", "good", 0.96),
        ("Slow zones", "problem", 240),
    ]
    assert merged["notes"] == ["Slow zones: Latest data is from Oct 3."]

    assert (
        speed_and_slow_zones(judged("room", 0.9, "higher", (1, 0.95, 0.88)), judged("win", 10, "lower", (30, 60, 180)))[
            "status"
        ]
        == "room"
    )


def test_worst_of_lets_the_measured_part_decide():
    no_baseline = judged("insufficient", None, "higher", (1, 0.95, 0.88))
    assert speed_and_slow_zones(no_baseline, judged("good", 45, "lower", (30, 60, 180)))["status"] == "good"
    assert (
        speed_and_slow_zones(no_baseline, judged("insufficient", None, "lower", (30, 60, 180)))["status"]
        == "insufficient"
    )


def test_headline_ranks_a_combined_cell_by_its_furthest_part():
    speed = judged("good", 0.96, "higher", (1, 0.95, 0.88))
    combined = speed_and_slow_zones(speed, judged("room", 170, "lower", (30, 60, 180)))
    # Slow zones sit 183% past good, further than service's 8%.
    held_back = headline_for([cell("service", "room", 0.85), combined], 2)["heldBackBy"]
    assert held_back == ["speed", "service"]


def line(weekday: list[int], saturday: list[int] | None = None, regime: str = "current") -> dict:
    days = {"weekday": {"cancelled": False, "tripsPerHour": weekday}}
    if saturday is not None:
        days["saturday"] = {"cancelled": False, "tripsPerHour": saturday}
    return {"serviceRegimes": {regime: days}}


def hours(**by_hour: int) -> list[int]:
    tph = [0] * 24
    for hour, trips in by_hour.items():
        tph[int(hour[1:])] = trips
    return tph


def test_goal_coverage_counts_gaps_and_service_after_midnight():
    # 5 AM to 1 AM: 21 service hours, a gap at noon, and 1 AM belongs to the day before.
    tph = hours(**{f"h{h}": 6 for h in range(5, 24) if h != 12}, h0=4, h1=2)
    coverage = goal_coverage(line(tph), 10)
    assert coverage.total == 21 * 5
    assert coverage.met == 18 * 5


def test_goal_coverage_weights_weekdays_five_times():
    coverage = goal_coverage(line(hours(h8=6, h9=6), saturday=hours(h8=2, h9=2)), 10)
    assert (coverage.met, coverage.total) == (10, 12)


def test_combine_coverage_pools_hours_or_weights_by_riders():
    busy = goal_coverage(line(hours(h8=6, h9=6)), 10)  # 10 of 10
    quiet = goal_coverage(line(hours(h8=1, h9=1, h10=1, h11=1)), 10)  # 0 of 20
    assert combine_coverage([(busy, None), (quiet, None)]) == pytest.approx(10 / 30)
    assert combine_coverage([(busy, 900), (quiet, 100)]) == pytest.approx(0.9)
    assert combine_coverage([]) is None


def test_bus_fleet_weights_the_share_by_trips_over_four_weeks():
    result = bus_fleet_cell(
        [
            *daily("2026-08-03", 14, fleet_trips=100, fleet_mix_hybrid=0, avg_bus_age=20),
            *daily("2026-09-14", 26, fleet_trips=1, fleet_mix_hybrid=50, avg_bus_age=9),
            *daily("2026-10-10", 1, fleet_trips=900, fleet_mix_battery=10, fleet_mix_hybrid=80),
        ],
        now=NOW,
    )
    assert result["score"] == pytest.approx((26 * 0.5 + 900 * 0.9) / 926)
    assert (result["direction"], result["thresholds"], result["scoreUnit"]) == ("higher", [0.9, 0.75, 0.5], "pct")
    assert result["secondary"] == "Battery 10% · Hybrid 79% of trips"
    assert result["baselineLabel"] == "Average bus age: 9 yr"
    assert result["trend"] is None
    assert result["status"] == "good"


def test_bus_fleet_needs_four_weeks_and_says_why_it_has_no_status():
    assert bus_fleet_cell(daily("2026-09-21", 20, fleet_trips=100, fleet_mix_hybrid=95), now=NOW)["status"] == (
        "insufficient"
    )
    assert bus_fleet_cell(None, now=NOW)["notes"] == ["Couldn't load bus fleet data."]
    empty = bus_fleet_cell(daily("2026-09-01", 30), now=NOW)
    assert empty["score"] is None
    assert "No recent fleet data." in empty["notes"]


def headway_rows(start: str, days: int, within: int, outside: int) -> list[dict]:
    return daily(
        start,
        days,
        headways_under_50=outside,
        headways_50_75=0,
        headways_75_125=within,
        headways_125_150=0,
        headways_over_150=0,
    )


def test_reliability_pools_counts_over_the_last_four_complete_weeks():
    result = reliability_cell(
        [
            *headway_rows("2026-07-13", 49, within=10, outside=90),  # older weeks, outside the score
            *headway_rows("2026-08-31", 21, within=60, outside=40),
            *headway_rows("2026-09-21", 14, within=1, outside=0),  # few headways barely move it
            *headway_rows("2026-10-05", 6, within=0, outside=1000),  # this week isn't complete yet
        ],
        now=NOW,
    )
    assert result["score"] == pytest.approx((2 * 7 * 60 + 14) / (2 * 7 * 100 + 14))
    assert result["asOf"] == "2026-09-28"
    assert result["secondary"] == "60% within 50–150% of schedule"
    assert result["baselineLabel"] == "40% bunched (half the scheduled gap or less)"
    assert (result["direction"], result["scoreUnit"]) == ("higher", "pct")


def test_reliability_needs_four_weeks_and_says_why_it_has_no_status():
    assert reliability_cell(headway_rows("2026-09-14", 21, within=9, outside=1), now=NOW)["status"] == "insufficient"
    assert reliability_cell(None, now=NOW)["notes"] == ["Couldn't load headway data."]
    empty = reliability_cell(daily("2026-09-01", 30), now=NOW)
    assert empty["score"] is None
    assert "No recent headway data." in empty["notes"]


def cr_rows(start: str, days: int, on_time: int, trips: int, cancelled: int = 0) -> list[dict]:
    peak = {"otpNumerator": on_time // 2, "otpDenominator": trips // 2}
    return daily(start, days, otpNumerator=on_time, otpDenominator=trips, cancelled=cancelled, peak=peak)


def test_cr_reliability_uses_the_last_four_complete_weeks_of_the_mbtas_batch():
    result = cr_reliability_cell(
        [
            *cr_rows("2026-05-04", 28, on_time=50, trips=100),  # older weeks, outside the score
            *cr_rows("2026-06-01", 28, on_time=90, trips=100, cancelled=1),
            *cr_rows("2026-06-29", 2, on_time=0, trips=100),  # the batch ends partway through this week
        ],
        now=NOW,
    )
    assert result["score"] == pytest.approx(0.9)
    assert result["status"] == "room"
    assert result["asOf"] == "2026-06-22"
    assert result["secondary"] == "90% on time at peak"
    assert result["baselineLabel"] == "28 trains cancelled"
    assert result["thresholds"] == [0.95, 0.92, 0.85]
    assert result["notes"][-1] == "The MBTA publishes these in batches; the latest is from Jun 30."


def test_cr_reliability_goes_stale_and_says_why_it_has_no_status():
    assert cr_reliability_cell(cr_rows("2026-03-02", 28, on_time=99, trips=100), now=NOW)["status"] == "insufficient"
    assert cr_reliability_cell(None, now=NOW)["notes"] == ["Couldn't load on-time data."]
    assert cr_reliability_cell([], now=NOW)["notes"] == ["No recent on-time data."]


def service_ridership() -> dict:
    regime = {"weekday": {"cancelled": False, "tripsPerHour": hours(**{f"h{h}": 8 for h in range(5, 24)})}}
    bus_regime = {"weekday": {"cancelled": False, "tripsPerHour": hours(**{f"h{h}": 4 for h in range(6, 22)})}}
    history = {d: 100 for d, _ in weekly("2026-07-13", [0] * 13)}
    return {
        "summaryData": {"endDate": "2026-10-10", "totalServiceHistory": history},
        "modeData": {"bus": {"totalServiceHistory": history}},
        "lineData": {
            "line-Red": {"id": "line-Red", "lineKind": "red", "serviceRegimes": {"current": regime}},
            "line-1": {
                "id": "line-1",
                "lineKind": "bus",
                "routeIds": ["1"],
                "serviceHistory": history,
                "ridershipHistory": history,
                "serviceRegimes": {"current": bus_regime},
            },
            "line-9": {"id": "line-9", "lineKind": "bus", "routeIds": ["9"], "serviceRegimes": {"current": bus_regime}},
        },
    }


def test_build_overview_end_to_end():
    weeks = weekly("2026-07-13", [None] * 12)
    trips = [
        {
            "date": day,
            "count": 200,
            "miles_covered": 1000,
            "total_time": 180000,
            "avg_car_age": 20,
            "fleet_mix_red1": 0.5,
            "fleet_mix_red2": 30,
            "fleet_mix_red3": 69.5,
        }
        for day, _ in weeks
    ]
    fleet_rows = daily("2026-09-07", 34, fleet_trips=100, fleet_mix_hybrid=80)
    src = sources.Sources(
        trip_metrics={"line-red": trips},
        baselines={"metrics": {"service": {"series": {"line-red": {"value": 200, "bestWindow": None}}}}},
        delay_totals={"data": [{"date": f"{d['date']}T00:00:00", "Red": 45} for d in daily("2026-07-19", 84)]},
        service_ridership=service_ridership(),
        bus_fleet={"all": fleet_rows, "1": fleet_rows},
        now=NOW,
        subway_headways={"line-red": headway_rows("2026-08-31", 41, within=70, outside=30)},
        bus_headways={"1": headway_rows("2026-08-31", 41, within=50, outside=50), "9": daily("2026-08-31", 41)},
        cr_reliability=cr_rows("2026-06-01", 30, on_time=93, trips=100),
    )
    overview = build.build_overview(src)
    rows = {row["id"]: row for row in overview["rows"]}

    red = rows["red"]["cells"]
    assert red["service"]["status"] == "win"
    assert red["frequency"]["score"] == 1
    # No speed baseline, so 45 s a day of slow zones decides it.
    assert red["speed"]["status"] == "good"
    assert [(part["label"], part["status"]) for part in red["speed"]["parts"]] == [
        ("Speed", "insufficient"),
        ("Slow zones", "good"),
    ]
    assert "slowZones" not in red
    # Pullman-Standard is under 1% of cars, so UTDC is the oldest still in service.
    assert red["fleet"]["secondary"] == "Oldest in service: UTDC (1987–89), 30% of cars"
    assert red["fleet"]["score"] == 2026 - 1987

    assert red["reliability"]["score"] == pytest.approx(0.7)
    assert red["reliability"]["status"] == "good"
    assert red["reliability"]["href"] == "/red/reliability"
    assert list(red) == ["service", "frequency", "reliability", "speed", "fleet"]
    assert rows["blue"]["cells"]["reliability"]["status"] == "insufficient"
    bus_reliability = rows["all-bus"]["cells"]["reliability"]
    assert bus_reliability["score"] == pytest.approx(0.5)
    assert bus_reliability["notes"] == ["Mid-route timepoints only.", "1 of 2 routes have headway data."]
    cr_reliability = rows["commuter-rail"]["cells"]["reliability"]
    assert (cr_reliability["status"], cr_reliability["href"]) == ("good", "/commuter-rail/reliability")
    assert rows["ferry"]["cells"]["reliability"]["status"] == "na"

    frequent = rows["frequent-bus"]["cells"]["fleet"]
    assert frequent["score"] == pytest.approx(0.8)
    assert frequent["notes"][0] == "1 of 2 routes have fleet data."
    assert rows["all-bus"]["cells"]["fleet"]["status"] == "good"
    assert rows["ferry"]["headline"]["kind"] == "limited"
    assert [stat["id"] for stat in overview["hero"]] == [
        "subwayFrequency",
        "service",
        "slowZones",
        "frequentBus",
        "crHeadway",
    ]
    assert "trend" not in overview["hero"][0]
    # Valid JSON: no NaN or infinity anywhere.
    build.to_json(overview)
