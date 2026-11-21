from datetime import date

from ..service_ridership_dashboard.summary import _latest_week_all_lines_report, get_summary_data


def _line(ridership, trips=10, service_weeks=("2026-10-26",)):
    regime = {"weekday": {"cancelled": False, "totalTrips": trips}}
    return {
        "ridershipHistory": ridership,
        "serviceHistory": {week: trips for week in service_weeks},
        "serviceRegimes": {"current": regime, "oneYearAgo": regime},
    }


def _summary(*lines):
    return get_summary_data(list(lines), date(2026, 1, 1), date(2026, 11, 20))


def test_total_passengers_uses_the_latest_week_every_line_reports():
    """Bus a week ahead of subway used to make the headline just bus (#263)."""
    subway = _line({"2026-10-19": 100, "2026-10-26": 110})
    bus = _line({"2026-10-26": 50, "2026-11-02": 55})
    summary = _summary(subway, bus)
    assert summary["totalPassengers"] == 160


def test_history_ends_at_the_latest_week_every_line_reports():
    """The All Lines chart fell to bus alone in the weeks subway hadn't reported (t-performance-dash#1610)."""
    subway = _line({"2026-10-19": 100, "2026-10-26": 110})
    bus = _line({"2026-10-19": 45, "2026-10-26": 50, "2026-11-02": 55})
    summary = _summary(subway, bus)
    assert summary["totalRidershipHistory"] == {"2026-10-19": 145, "2026-10-26": 160}


def test_weeks_a_running_line_skipped_are_left_out():
    """Subway had no 2026-11-02 row, so that week's total was bus alone."""
    weeks = ("2026-10-26", "2026-11-02", "2026-11-09")
    subway = _line({"2026-10-26": 110, "2026-11-09": 120}, service_weeks=weeks)
    bus = _line({"2026-10-26": 50, "2026-11-02": 55, "2026-11-09": 60}, service_weeks=weeks)
    summary = _summary(subway, bus)
    assert summary["totalRidershipHistory"] == {"2026-10-26": 160, "2026-11-09": 180}
    assert summary["totalPassengers"] == 180


def test_a_skipped_week_isnt_the_headline_while_another_line_lags():
    """Beta on 2026-11-20: subway skipped 2026-11-02 and bus hadn't reached 2026-11-09."""
    weeks = ("2026-10-26", "2026-11-02", "2026-11-09")
    subway = _line({"2026-10-26": 110, "2026-11-09": 120}, service_weeks=weeks)
    bus = _line({"2026-10-26": 50, "2026-11-02": 55}, service_weeks=weeks)
    summary = _summary(subway, bus)
    assert summary["totalRidershipHistory"] == {"2026-10-26": 160}
    assert summary["totalPassengers"] == 160


def test_weeks_a_small_route_skipped_are_kept():
    """Route 712/713 skipping Labor Day week would otherwise hide that week's dip."""
    weeks = ("2026-08-31", "2026-09-07", "2026-09-14")
    small_route = _line({"2026-08-31": 2, "2026-09-14": 2}, service_weeks=weeks)
    subway = _line({"2026-08-31": 100, "2026-09-07": 80, "2026-09-14": 100}, service_weeks=weeks)
    summary = _summary(small_route, subway)
    assert summary["totalRidershipHistory"] == {"2026-08-31": 102, "2026-09-07": 80, "2026-09-14": 102}


def test_gaps_without_service_are_kept():
    """A seasonal ferry has no riders in winter because it doesn't run, so those weeks are complete."""
    ferry = _line({"2026-01-05": 5, "2026-05-04": 8}, service_weeks=("2026-01-05", "2026-05-04"))
    subway = _line({week: 100 for week in ("2026-01-05", "2026-02-02", "2026-05-04")})
    summary = _summary(ferry, subway)
    assert summary["totalRidershipHistory"] == {"2026-01-05": 105, "2026-02-02": 100, "2026-05-04": 108}


def test_lines_that_stopped_reporting_long_ago_dont_hold_back_the_total():
    weeks = _latest_week_all_lines_report([{"2026-06-22": 1}, {"2026-10-26": 1, "2026-11-02": 1}, {"2026-11-02": 1}])
    assert weeks == "2026-11-02"
    regional_rail = _line({"2026-06-22": 30})
    bus = _line({"2026-06-22": 50, "2026-11-02": 55})
    assert _summary(regional_rail, bus)["totalRidershipHistory"] == {"2026-06-22": 80, "2026-11-02": 55}


def test_no_ridership():
    assert _latest_week_all_lines_report([{}, None or {}]) is None
    summary = _summary(_line(None))
    assert summary["totalPassengers"] == 0
    assert summary["totalRidershipHistory"] == {}
