from datetime import date

from .. import constants
from ..delays import process
from ..delays.process import (
    alert_is_delay,
    generate_requests,
    is_line_active,
    process_delay_time,
)
from ..delays.types import Alert


def make_alert(text: str) -> Alert:
    return Alert(valid_from="2024-01-01T08:00:00", valid_to="2024-01-01T09:00:00", text=text)


# --- alert_is_delay ---


def test_alert_is_delay_subway_pattern():
    assert alert_is_delay(make_alert("Red Line experiencing delays of about 10 minutes")) is True
    assert alert_is_delay(make_alert("Experiencing delays of about 25 minutes due to signal problems")) is True


def test_alert_is_delay_commuter_rail_patterns():
    assert alert_is_delay(make_alert("Train is running 15 minutes late")) is True
    assert alert_is_delay(make_alert("Train is 20 minutes behind schedule")) is True
    assert alert_is_delay(make_alert("Train is behind schedule at North Station")) is True


def test_alert_is_delay_non_delay():
    assert alert_is_delay(make_alert("Service suspended due to flooding")) is False
    assert alert_is_delay(make_alert("Shuttle buses replacing service")) is False
    assert alert_is_delay(make_alert("")) is False


def test_alert_is_delay_requires_both_keywords_for_subway():
    # "delays" alone without "minutes" is not a delay
    assert alert_is_delay(make_alert("Experiencing delays at Park Street")) is False
    # "minutes" alone without "delays" is also not the subway pattern (but could be CR)
    # "30 minutes" without "late" or "behind schedule" is not CR pattern
    assert alert_is_delay(make_alert("Travel time approximately 30 minutes")) is False


# --- is_line_active ---


def test_is_line_active_cr_middleborough_before_cutoff():
    before = constants.CR_MIDDLEBOROUGH_DISCONTINUED.replace(day=constants.CR_MIDDLEBOROUGH_DISCONTINUED.day - 1)
    assert is_line_active("CR-Middleborough", before) is True


def test_is_line_active_cr_middleborough_on_cutoff():
    cutoff = constants.CR_MIDDLEBOROUGH_DISCONTINUED
    assert is_line_active("CR-Middleborough", cutoff) is False


def test_is_line_active_cr_newbedford_before_cutoff():
    before = constants.CR_MIDDLEBOROUGH_DISCONTINUED.replace(day=constants.CR_MIDDLEBOROUGH_DISCONTINUED.day - 1)
    assert is_line_active("CR-NewBedford", before) is False


def test_is_line_active_cr_newbedford_on_cutoff():
    cutoff = constants.CR_MIDDLEBOROUGH_DISCONTINUED
    assert is_line_active("CR-NewBedford", cutoff) is True


def test_is_line_active_regular_lines_always_active():
    some_date = date(2020, 1, 1)
    for line in ["Red", "Orange", "Blue", "Green-B", "CR-Framingham"]:
        assert is_line_active(line, some_date) is True


# --- generate_requests ---


def test_generate_requests_single_day():
    reqs = generate_requests(date(2024, 6, 1), date(2024, 6, 1), lines=["Red", "Blue"])
    assert len(reqs) == 2
    assert {r.route for r in reqs} == {"Red", "Blue"}
    assert all(r.date == date(2024, 6, 1) for r in reqs)


def test_generate_requests_multi_day():
    reqs = generate_requests(date(2024, 6, 1), date(2024, 6, 3), lines=["Red"])
    assert len(reqs) == 3
    assert [r.date for r in reqs] == [date(2024, 6, 1), date(2024, 6, 2), date(2024, 6, 3)]


def test_generate_requests_skips_inactive_line():
    # CR-Middleborough is discontinued after a certain date; CR-NewBedford before
    cutoff = constants.CR_MIDDLEBOROUGH_DISCONTINUED
    reqs = generate_requests(cutoff, cutoff, lines=["CR-Middleborough", "CR-NewBedford"])
    routes = {r.route for r in reqs}
    assert "CR-Middleborough" not in routes
    assert "CR-NewBedford" in routes


# --- process_delay_time ---


def test_process_delay_time_empty():
    total, by_type = process_delay_time([])
    assert total == 0
    assert all(v == 0 for v in by_type.values())


def test_process_delay_time_non_delay_alerts():
    alerts = [make_alert("Service suspended at Park Street")]
    total, by_type = process_delay_time(alerts)
    assert total == 0


def test_process_delay_time_subway_pattern():
    alerts = [make_alert("Red Line experiencing delays of about 10 minutes due to a disabled train")]
    total, by_type = process_delay_time(alerts)
    assert total == 10
    assert by_type["disabled_vehicle"] == 10


def test_process_delay_time_cr_minutes_late():
    alerts = [make_alert("Train is running 20 minutes late due to signal problems at North Station")]
    total, by_type = process_delay_time(alerts)
    assert total == 20
    assert by_type["signal_problem"] == 20


def test_process_delay_time_range_pattern_takes_max():
    # "10-15 minutes behind schedule" → max = 15
    alerts = [make_alert("Train is 10-15 minutes behind schedule due to a disabled train")]
    total, by_type = process_delay_time(alerts)
    assert total == 15
    assert by_type["disabled_vehicle"] == 15


def test_process_delay_time_multiple_alerts():
    alerts = [
        make_alert("Red Line experiencing delays of about 10 minutes due to a signal problem"),
        make_alert("Orange Line experiencing delays of about 5 minutes due to a disabled train"),
    ]
    total, by_type = process_delay_time(alerts)
    assert total == 15
    assert by_type["signal_problem"] == 10
    assert by_type["disabled_vehicle"] == 5


def test_process_delay_time_unknown_type_goes_to_other():
    alerts = [make_alert("Red Line experiencing delays of about 7 minutes due to heavy ridership")]
    total, by_type = process_delay_time(alerts)
    assert total == 7
    assert by_type["other"] == 7


def test_process_delay_time_bus_traffic_and_accident():
    alerts = [
        make_alert("Route 39 is experiencing delays of about 20 minutes due to traffic."),
        make_alert("Route 14 is experiencing delays of about 30 minutes due to an accident."),
    ]
    total, by_type = process_delay_time(alerts, bus=True)
    assert total == 50
    assert by_type["car_traffic"] == 50


def test_process_delay_time_more_than():
    alerts = [make_alert("Route 108 is experiencing delays of more than 30 minutes due to a disabled bus.")]
    total, by_type = process_delay_time(alerts, bus=True)
    assert total == 30
    assert by_type["disabled_vehicle"] == 30


# --- process_requests: CommuterRail ---


def test_process_requests_commuter_rail_counts_shared_alert_once(monkeypatch):
    shared = make_alert("Trains are running 20 minutes behind schedule due to a signal problem.")
    by_line = {
        "CR-Providence": [shared, make_alert("Providence Train 801 is 10 minutes late.")],
        "CR-Franklin": [shared],
    }
    monkeypatch.setattr(process, "process_single_day", lambda request: by_line[request.route])
    day = date(2024, 1, 1)
    lines = list(by_line)
    dfs = process.process_requests(generate_requests(day, day, lines), lines)

    assert dfs["CR-Providence"].iloc[0]["total_delay_time"] == 30
    assert dfs["CR-Franklin"].iloc[0]["total_delay_time"] == 20
    total = dfs[process.COMMUTER_RAIL_AGGREGATE].iloc[0]
    assert total["line"] == process.COMMUTER_RAIL_AGGREGATE
    assert total["total_delay_time"] == 30
    assert "delay_alert_minutes" not in dfs[process.COMMUTER_RAIL_AGGREGATE].columns


def test_process_requests_commuter_rail_quiet_day_writes_zero(monkeypatch):
    monkeypatch.setattr(process, "process_single_day", lambda request: [])
    day = date(2024, 1, 1)
    dfs = process.process_requests(generate_requests(day, day, ["CR-Lowell"]), ["CR-Lowell"])
    assert dfs[process.COMMUTER_RAIL_AGGREGATE].iloc[0]["total_delay_time"] == 0


def test_process_requests_no_commuter_rail_without_cr_lines(monkeypatch):
    monkeypatch.setattr(process, "process_single_day", lambda request: [])
    monkeypatch.setattr(process.duration, "load_lamp_alerts", lambda day: None)
    day = date(2024, 1, 1)
    dfs = process.process_requests(generate_requests(day, day, ["Red"]), ["Red"])
    assert process.COMMUTER_RAIL_AGGREGATE not in dfs


# --- process_requests: Green ---


def _lamp_alert(alert_id, header, start, closed, routes):
    return {
        alert_id: {
            "attributes": {
                "effect": "DELAY",
                "header": header,
                "created_at": start,
                "closed_timestamp": closed,
                "active_period": [{"start": start, "end": None}],
                "informed_entity": [{"route": route} for route in routes],
            }
        }
    }


def test_process_requests_green_counts_trunk_alert_once(monkeypatch):
    trunk = make_alert("Green Line: Delays of about 15 minutes due to a disabled train at Park Street.")
    branch = make_alert("Green Line D Branch: Delays of about 10 minutes due to a signal problem.")
    by_line = {"Green-B": [trunk], "Green-C": [trunk], "Green-D": [trunk, branch], "Green-E": [trunk]}
    trunk_routes = ["Green-B", "Green-C", "Green-D", "Green-E"]
    lamp = {
        **_lamp_alert("1", trunk["text"], "2024-01-01T08:00:00-05:00", "2024-01-01T08:30:00-05:00", trunk_routes),
        # Overlaps the trunk alert by 10 minutes, then runs 20 more on its own
        **_lamp_alert("2", branch["text"], "2024-01-01T08:20:00-05:00", "2024-01-01T08:50:00-05:00", ["Green-D"]),
    }
    monkeypatch.setattr(process, "process_single_day", lambda request: by_line[request.route])
    monkeypatch.setattr(process.duration, "load_lamp_alerts", lambda day: lamp)
    day = date(2024, 1, 1)
    lines = list(by_line)
    dfs = process.process_requests(generate_requests(day, day, lines), lines)

    assert dfs["Green-B"].iloc[0]["delay_alert_minutes"] == 30
    assert dfs["Green-D"].iloc[0]["delay_alert_minutes"] == 50
    green = dfs[process.GREEN_AGGREGATE].iloc[0]
    assert green["line"] == process.GREEN_AGGREGATE
    assert green["total_delay_time"] == 25
    assert green["delay_alert_minutes"] == 50
    assert green["delay_alert_minutes_disabled_vehicle"] == 30


def test_process_requests_green_without_lamp_file_has_no_live_minutes(monkeypatch):
    monkeypatch.setattr(process, "process_single_day", lambda request: [])
    monkeypatch.setattr(process.duration, "load_lamp_alerts", lambda day: None)
    day = date(2024, 1, 1)
    dfs = process.process_requests(generate_requests(day, day, ["Green-B", "Green-C"]), ["Green-B", "Green-C"])
    green = dfs[process.GREEN_AGGREGATE]
    assert green.iloc[0]["total_delay_time"] == 0
    assert "delay_alert_minutes" not in green.columns
