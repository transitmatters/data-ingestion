from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from mbta_gtfs_sqlite.models import Base, CalendarService, CalendarServiceException, StopTime, Trip
from mbta_gtfs_sqlite.models.calendar import ServiceDayAvailability
from mbta_gtfs_sqlite.models.calendar_dates import CalendarServiceExceptionType
from sqlalchemy import Integer, create_engine
from sqlalchemy.orm import Session

from ..headway_reliability import events, ingest, schedule
from ..headway_reliability.buckets import HEADWAY_FIELDS
from ..headway_reliability.compute import headway_counts
from ..headway_reliability.trips import line_trip_counts, trains_seen


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


def visits(trips: list[str], service_date="2026-11-05", event_types=("ARR", "DEP")) -> pd.DataFrame:
    """Events for trains stopping at one stop, one row per trip and event type."""
    return pd.DataFrame(
        [
            {
                "service_date": service_date,
                "trip_id": trip,
                "direction_id": "0",
                "event_type": kind,
                "event_time": "2026-11-05 08:00:00",
                "scheduled_headway": "600",
            }
            for trip in trips
            for kind in event_types
        ]
    )


def test_trains_seen_counts_distinct_trips_that_stopped_including_added_ones():
    events_at_stop = pd.concat(
        [
            visits(["t1", "t2", "ADDED-1"]),
            visits(["t3"], event_types=("PRA",)),  # a prediction, not a train
            visits(["t1"], service_date="2026-11-06"),
        ]
    )
    assert trains_seen(events_at_stop) == {"2026-11-05": 3, "2026-11-06": 1}


def test_a_line_takes_its_typical_stop_so_one_stop_missing_data_cant_move_it():
    stops = {"0": ["a", "b", "c"], "1": ["d"]}
    scheduled = {("a", "Red"): 100, ("b", "Red"): 100, ("c", "Red"): 100, ("d", "Red"): 50}
    # Stop b lost half its events; c ran an extra train, which doesn't make up for a in its place.
    trains = {"a": 90, "b": 50, "c": 101, "d": 50}
    assert line_trip_counts("line-red", stops, scheduled, trains) == {"scheduled_trips": 150, "dropped_trips": 10}
    # A stop with no events that day is left out, not counted as every trip dropped.
    assert line_trip_counts("line-red", stops, scheduled, {**trains, "b": None}) == {
        "scheduled_trips": 150,
        "dropped_trips": 5,
    }


def test_a_line_needs_half_its_stops_in_every_direction():
    stops = {"0": ["a", "b", "c"], "1": ["d"]}
    scheduled = {("a", "Red"): 100, ("b", "Red"): 100, ("c", "Red"): 100, ("d", "Red"): 50}
    assert line_trip_counts("line-red", stops, scheduled, {"a": 90, "b": None, "c": None, "d": 50}) is None
    assert line_trip_counts("line-red", stops, scheduled, {"a": 90, "b": 90, "c": 90, "d": None}) is None
    assert line_trip_counts("line-red", stops, {}, {"a": 90}) is None


def test_green_branches_count_at_their_own_stops_and_add_up():
    stops = {"0": ["trunk", "b1", "b2", "e1"], "1": []}
    scheduled = {
        ("trunk", "Green-B"): 100,
        ("trunk", "Green-E"): 80,
        ("b1", "Green-B"): 100,
        ("b2", "Green-B"): 100,
        ("e1", "Green-E"): 80,
    }
    # Trunk trains can't be told apart by branch, so the trunk stop doesn't count.
    trains = {"trunk": 0, "b1": 90, "b2": 90, "e1": 80}
    assert line_trip_counts("line-green", stops, scheduled, trains) == {"scheduled_trips": 180, "dropped_trips": 10}


def test_items_get_trip_counts_from_the_schedule(monkeypatch):
    lamp = "Events-lamp/daily-data/70057/Year=2026/Month=11/Day=5/events.csv"
    fake_bucket(monkeypatch, {lamp: pd.concat([departures([10, 10]), visits(["t0", "t1"])])}, archived=False)

    days = [date(2026, 11, 5)]
    [blue] = ingest.compute_days(days, {date(2026, 11, 5): {("70057", "Blue"): 4}})
    assert (blue["scheduled_trips"], blue["dropped_trips"]) == (4, 1)  # trips t0, t1 and t2 stopped
    [without] = ingest.compute_days(days)
    assert "scheduled_trips" not in without


def test_headways_are_written_even_when_the_schedule_fails(monkeypatch):
    lamp = "Events-lamp/daily-data/70057/Year=2026/Month=11/Day=5/events.csv"
    fake_bucket(monkeypatch, {lamp: departures([10, 10])}, archived=False)
    written = []
    monkeypatch.setattr(ingest, "write_items", written.extend)

    def unreachable(*_):
        raise ConnectionError("no feed")

    monkeypatch.setattr(schedule, "scheduled_trips", unreachable)
    with pytest.raises(RuntimeError, match="without trip counts"):
        ingest.update_days(date(2026, 11, 5), date(2026, 11, 5))
    assert [item["route"] for item in written] == ["line-blue"]


def add(session: Session, model, **values):
    """A row with every required column filled, so a test only names the ones it cares about."""
    for column in model.__table__.columns:
        if not column.nullable and not column.primary_key and column.name not in values:
            values[column.name] = 1 if column.name == "feed_info_id" else 0 if isinstance(column.type, Integer) else ""
    session.add(model(**values))


def test_schedule_counts_trips_through_each_stop_on_the_day():
    session = Session(create_engine("sqlite://"))
    Base.metadata.create_all(session.bind)
    on, off = ServiceDayAvailability.AVAILABLE, ServiceDayAvailability.NOT_AVAILABLE
    for service, weekdays in [("weekday", [on] * 5 + [off] * 2), ("holiday", [off] * 7)]:
        days = dict(zip(["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"], weekdays))
        add(
            session,
            CalendarService,
            service_id=service,
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 30),
            **days,
        )
    holiday = date(2026, 11, 26)
    add(
        session,
        CalendarServiceException,
        service_id="weekday",
        date=holiday,
        exception_type=CalendarServiceExceptionType.REMOVED,
    )
    add(
        session,
        CalendarServiceException,
        service_id="holiday",
        date=holiday,
        exception_type=CalendarServiceExceptionType.ADDED,
    )
    for trip, route, service in [
        ("r1", "Red", "weekday"),
        ("r2", "Red", "weekday"),
        ("r3", "Red", "holiday"),
        ("bus", "1", "weekday"),
    ]:
        add(session, Trip, trip_id=trip, route_id=route, service_id=service)
        add(session, StopTime, trip_id=trip, stop_id="70077")
    add(session, StopTime, trip_id="r1", stop_id="70061")
    session.commit()

    calendar = schedule._calendar(session)
    stops = {"70077", "70061"}
    assert schedule._scheduled_on(session, calendar, date(2026, 11, 25), stops) == {
        ("70077", "Red"): 2,
        ("70061", "Red"): 1,
    }
    assert schedule._scheduled_on(session, calendar, holiday, stops) == {("70077", "Red"): 1}
