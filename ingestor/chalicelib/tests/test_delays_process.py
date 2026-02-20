import pandas as pd
import pytest
from datetime import date

from .. import constants
from ..delays.process import (
    aggregate_commuter_rail_df,
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


# --- aggregate_commuter_rail_df ---


def _make_line_df(line: str, dates=None):
    if dates is None:
        dates = ["2024-01-01", "2024-01-02"]
    df = pd.DataFrame(
        {
            "total_delay_time": [10, 5],
            "line": [line, line],
        },
        index=pd.to_datetime(dates),
    )
    return df


def test_aggregate_commuter_rail_df_no_cr_lines():
    df_data = {"Red": _make_line_df("Red"), "Blue": _make_line_df("Blue")}
    result = aggregate_commuter_rail_df(df_data)
    assert result is None


def test_aggregate_commuter_rail_df_single_cr_line():
    df_data = {"CR-Framingham": _make_line_df("CR-Framingham")}
    result = aggregate_commuter_rail_df(df_data)
    assert result is not None
    assert (result["line"] == "CommuterRail").all()


def test_aggregate_commuter_rail_df_multiple_cr_lines():
    df_data = {
        "CR-Framingham": _make_line_df("CR-Framingham"),
        "CR-Worcester": _make_line_df("CR-Worcester"),
        "Red": _make_line_df("Red"),
    }
    result = aggregate_commuter_rail_df(df_data)
    assert result is not None
    assert len(result) == 4  # 2 rows from each CR line
    assert (result["line"] == "CommuterRail").all()
