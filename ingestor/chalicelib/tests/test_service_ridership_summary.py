from datetime import date

from ..service_ridership_dashboard.summary import _latest_week_all_lines_report, get_summary_data


def _line(ridership, trips=10):
    regime = {"weekday": {"cancelled": False, "totalTrips": trips}}
    return {
        "ridershipHistory": ridership,
        "serviceHistory": {"2026-10-26": trips},
        "serviceRegimes": {"current": regime, "oneYearAgo": regime},
    }


def test_total_passengers_uses_the_latest_week_every_line_reports():
    """Bus a week ahead of subway used to make the headline just bus (#263)."""
    subway = _line({"2026-10-19": 100, "2026-10-26": 110})
    bus = _line({"2026-10-26": 50, "2026-11-02": 55})
    summary = get_summary_data([subway, bus], date(2026, 1, 1), date(2026, 11, 8))
    assert summary["totalPassengers"] == 160
    assert summary["totalRidershipHistory"]["2026-11-02"] == 55  # the history itself is unchanged


def test_lines_that_stopped_reporting_long_ago_dont_hold_back_the_total():
    weeks = _latest_week_all_lines_report([{"2026-06-22": 1}, {"2026-10-26": 1, "2026-11-02": 1}, {"2026-11-02": 1}])
    assert weeks == "2026-11-02"


def test_no_ridership():
    assert _latest_week_all_lines_report([{}, None or {}]) is None
    summary = get_summary_data([_line(None)], date(2026, 1, 1), date(2026, 11, 8))
    assert summary["totalPassengers"] == 0
