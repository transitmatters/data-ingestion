from datetime import date, datetime, timedelta

import pandas as pd

from ..headway_reliability import events, ingest
from ..headway_reliability.buckets import HEADWAY_FIELDS
from ..headway_reliability.compute import headway_counts


def departures(gaps_min: list[float], scheduled_sec="600", start="2026-11-05 08:00:00", **overrides) -> pd.DataFrame:
    """DEP events at one stop, `gaps_min` apart, each on its own trip."""
    t = datetime.fromisoformat(start)
    times = [t]
    for gap in gaps_min:
        times.append(times[-1] + timedelta(minutes=gap))
    rows = [
        {
            "service_date": "2026-11-05",
            "trip_id": f"t{i}",
            "direction_id": "0",
            "event_type": "DEP",
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "scheduled_headway": scheduled_sec,
        }
        for i, time in enumerate(times)
    ]
    return pd.DataFrame(rows).assign(**overrides)


def counts(df: pd.DataFrame) -> dict[str, int]:
    result = headway_counts(df)
    return {field: int(result[field].sum()) for field in HEADWAY_FIELDS}


def test_buckets_match_the_dashboard_edges():
    # Against a 10 minute schedule: 0.5 is bunched, 0.75 and 1.25 fall outside "on time", 1.5 goes up.
    assert counts(departures([5, 7.5, 8, 12, 12.5, 15, 14])) == {
        "headways_under_50": 1,
        "headways_50_75": 1,
        "headways_75_125": 2,
        "headways_125_150": 2,
        "headways_over_150": 1,
    }


def test_skips_repeat_trips_long_gaps_and_missing_schedules():
    df = departures([10, 10, 130, 10, 10])
    df.loc[2, "trip_id"] = "t1"  # the same train departing twice
    df.loc[5, "scheduled_headway"] = None
    # Kept: t0->t1 and t3->t4. Dropped: the repeat, the 130 minute gap, and the unscheduled one.
    assert sum(counts(df).values()) == 2


def test_only_departures_count_and_directions_stay_apart():
    df = pd.concat(
        [
            departures([10, 10]),
            departures([10, 10]).assign(event_type="ARR"),
            departures([5, 5], start="2026-11-05 08:01:00", direction_id="1.0"),
        ]
    )
    result = headway_counts(df).set_index("direction_id")
    assert result.loc["0", "headways_75_125"] == 2
    assert result.loc["1", "headways_under_50"] == 2


def test_lamp_offsets_keep_the_fall_back_hour_in_order():
    df = pd.DataFrame(
        {
            "service_date": "2026-11-01",
            "trip_id": ["a", "b", "c"],
            "direction_id": "0",
            "event_type": "DEP",
            "event_time": ["2026-11-01 01:50:00-04:00", "2026-11-01 01:00:00-05:00", "2026-11-01 01:10:00-05:00"],
            "scheduled_headway": "600",
        }
    )
    assert counts(df)["headways_75_125"] == 2


def test_items_sum_stops_per_line_and_name_their_sources():
    stop = headway_counts(departures([10, 10]))
    other_direction = headway_counts(departures([4], direction_id="1"))
    items = ingest.to_items(
        [("line-red", "lamp", stop), ("line-red", "gobble", other_direction), ("line-blue", "lamp", stop)]
    )
    red = next(item for item in items if item["route"] == "line-red")
    assert red == {
        "route": "line-red",
        "date": "2026-11-05",
        "source": "gobble+lamp",
        "headways_under_50": 1,
        "headways_50_75": 0,
        "headways_75_125": 2,
        "headways_125_150": 0,
        "headways_over_150": 0,
        "byDirection": {
            "0": {**dict.fromkeys(HEADWAY_FIELDS, 0), "headways_75_125": 2},
            "1": {**dict.fromkeys(HEADWAY_FIELDS, 0), "headways_under_50": 1},
        },
    }
    assert [item["route"] for item in items] == ["line-blue", "line-red"]


def fake_bucket(monkeypatch, files: dict[str, pd.DataFrame], archived: bool):
    reads = []

    def read_events(key):
        reads.append(key)
        return files.get(key)

    monkeypatch.setattr(events, "read_events", read_events)
    monkeypatch.setattr(events, "month_archived", lambda day: archived)
    monkeypatch.setattr(ingest, "SUBWAY_STOPS", {"line-blue": {"0": ["70057"], "1": []}})
    return reads


def test_daily_reads_lamp_then_falls_back_to_gobble(monkeypatch):
    lamp = "Events-lamp/daily-data/70057/Year=2026/Month=11/Day=5/events.csv"
    gobble = "Events-live/daily-rapid-data/70057/Year=2026/Month=11/Day=6/events.csv.gz"
    files = {lamp: departures([10]), gobble: departures([10], start="2026-11-06 08:00:00", service_date="2026-11-06")}
    reads = fake_bucket(monkeypatch, files, archived=False)

    items = ingest.compute_days([date(2026, 11, 5), date(2026, 11, 6)])
    assert [(item["date"], item["source"]) for item in items] == [("2026-11-05", "lamp"), ("2026-11-06", "gobble")]
    assert gobble.replace("Day=6", "Day=5") not in reads  # LAMP had the 5th


def test_archived_months_read_one_file_and_keep_the_asked_days(monkeypatch):
    monthly = "Events/monthly-data/70057/Year=2026/Month=5/events.csv.gz"
    month = pd.concat(
        [departures([10], service_date=f"2026-05-0{d}", start=f"2026-05-0{d} 08:00:00") for d in (4, 5, 6)]
    )
    reads = fake_bucket(monkeypatch, {monthly: month}, archived=True)

    items = ingest.compute_days([date(2026, 5, 5), date(2026, 5, 6)])
    assert [(item["date"], item["source"]) for item in items] == [("2026-05-05", "monthly"), ("2026-05-06", "monthly")]
    assert reads == [monthly]
