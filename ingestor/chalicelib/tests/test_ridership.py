from datetime import date, timedelta
from tempfile import NamedTemporaryFile

import pandas as pd

from ..ridership.ingest import get_ridership_by_line_id
from ..ridership.process import (
    format_ridership_csv,
    format_subway_data,
    format_subway_station_data,
    pre_process_csv,
    unofficial_ferry_labels_map,
)
from ..ridership.rebuild_ferry_ride import add_lines_missing_from_gtfs, merge_ride_history, plan_changes


def _write_daily_csv(rows):
    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(rows, columns=["TripDate", "CompletedTrips"]).to_csv(path, index=False)
    return path


def _ride_weeks(rows):
    path = _write_daily_csv(rows)
    weeks = format_ridership_csv(
        pre_process_csv(path, "TripDate", None, "CompletedTrips", route_name="RIDE"),
        "TripDate",
        "Route",
        "CompletedTrips",
    )["RIDE"]
    return {week["date"]: week["count"] for week in weeks}


def test_pre_process_csv_sums_each_day():
    path = _write_daily_csv(
        [("2026/08/24 04:00:00+00", 1), ("2026/08/24 04:00:00+00", 2), ("2026/08/25 04:00:00+00", 4)]
    )
    df = pd.read_csv(pre_process_csv(path, "TripDate", None, "CompletedTrips", route_name="RIDE"))
    assert dict(zip(df["TripDate"], df["CompletedTrips"])) == {"2026-08-24": 3, "2026-08-25": 4}


def test_ride_weeks_are_weekday_averages_including_holiday_weeks():
    """The issue's repro: 100 trips a day used to give {'2026-08-31': 700, '2026-09-14': 700} (#255)."""
    rows = [((date(2026, 8, 31) + timedelta(days=i)).isoformat(), 100) for i in range(21)]
    # The Labor Day week (Mon 2026-09-07) is kept, from its four non-holiday weekdays
    assert _ride_weeks(rows) == {"2026-08-31": 100, "2026-09-07": 100, "2026-09-14": 100}


def test_ride_weeks_ignore_weekends_and_partial_weeks():
    # Weekdays 10, weekends 1000; the second week stops on Wednesday
    rows = [
        (d.isoformat(), 1000 if d.weekday() >= 5 else 10)
        for d in (date(2026, 9, 14) + timedelta(days=i) for i in range(10))
    ]
    assert _ride_weeks(rows) == {"2026-09-14": 10, "2026-09-21": 10}


def test_ride_weeks_handle_iso_year_boundary():
    # Mon 2025-12-29 through Sun 2026-01-04 is ISO week 1 of 2026; Jan 1 is a holiday
    rows = [("2025-12-29", 10), ("2025-12-30", 20), ("2025-12-31", 30), ("2026-01-01", 500), ("2026-01-02", 40)]
    assert _ride_weeks(rows) == {"2025-12-29": 25}


def test_merge_ride_history_takes_old_weeks_before_the_current_dataset():
    old = [{"date": "2025-08-18", "count": 1}, {"date": "2025-08-25", "count": 2}, {"date": "2025-09-01", "count": 3}]
    current = [{"date": "2025-09-01", "count": 30}, {"date": "2025-09-08", "count": 40}]
    assert merge_ride_history(old, current) == [
        {"date": "2025-08-18", "count": 1},
        {"date": "2025-08-25", "count": 2},
        {"date": "2025-09-01", "count": 30},
        {"date": "2025-09-08", "count": 40},
    ]


def test_plan_changes_deletes_rows_the_new_series_lacks():
    new = {"line-RIDE": [{"date": "2026-09-07", "count": 1}], "line-Boat-F8": []}
    existing = {
        "line-RIDE": [{"date": "2026-09-07"}, {"date": "2026-09-08"}],
        "line-Boat-F8": [{"date": "2025-06-02"}],
    }
    assert plan_changes(new, existing) == {"line-Boat-F8": ["2025-06-02"], "line-RIDE": ["2026-09-08"]}


def test_silver_line_gated_data_not_counted_as_bus():
    ridership_by_route_id = {
        "741": [{"date": "2026-09-07", "count": 100}],
        "1": [{"date": "2026-09-07", "count": 50}],
        "Silver Line": [{"date": "2026-09-07", "count": 90}, {"date": "2026-09-14", "count": 95}],
    }
    by_line_id = get_ridership_by_line_id(ridership_by_route_id, {})
    assert by_line_id["line-bus"] == [{"date": "2026-09-07", "count": 150}]


def test_routes_mapping_to_same_id_are_summed():
    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(
        [("2021-02-01", "F1", 10), ("2021-02-01", "F2H", 5), ("2021-02-08", "F2H", 7)],
        columns=["actual_departure", "route_id", "pax_on"],
    ).to_csv(path, index=False)
    by_route = format_ridership_csv(path, "actual_departure", "route_id", "pax_on", unofficial_ferry_labels_map)
    assert by_route == {
        "Boat-F1": [{"date": "2021-02-01", "count": 15.0}, {"date": "2021-02-08", "count": 7.0}],
    }


def test_holidays_are_left_out_of_the_peak_average():
    # Mon 2026-09-07 is Labor Day, so the week's peak average is Tue-Fri only
    rows = [("2026-09-07", "Red Line", 1000)] + [(f"2026-09-{d:02d}", "Red Line", 100) for d in range(8, 12)]
    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(rows, columns=["servicedate", "route_or_line", "validations"]).to_csv(path, index=False)
    assert format_subway_data(path)["Red"] == [{"date": "2026-09-07", "count": 100}]

    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(rows, columns=["date", "line", "riders"]).to_csv(path, index=False)
    assert format_ridership_csv(path, "date", "line", "riders") == {
        "Red Line": [{"date": "2026-09-07", "count": 100.0}]
    }


def _write_station_csv(rows):
    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(rows, columns=["servicedate", "stationname", "validations"]).to_csv(path, index=False)
    return path


def test_station_ridership_averages_peak_days_by_line():
    # Mon 2026-09-07 is Labor Day and Sat 2026-09-12 is a weekend, so only Tue-Fri count
    path = _write_station_csv(
        [("2026-09-07", "Harvard", 1000), ("2026-09-12", "Harvard", 1000)]
        + [(f"2026-09-{d:02d}", "Harvard", 100 + d) for d in range(8, 12)]
    )
    assert format_subway_station_data(path) == {
        "line-Red": [{"date": "2026-09-07", "stations": {"place-harsq": 110}}],
    }


def test_station_ridership_lists_transfer_stations_under_each_line():
    path = _write_station_csv([("2026-09-14", "Park Street", 500), ("2026-09-14", "Kenmore", 300)])
    by_line = format_subway_station_data(path)
    assert by_line["line-Red"] == [{"date": "2026-09-14", "stations": {"place-pktrm": 500}}]
    assert by_line["line-Green"] == [{"date": "2026-09-14", "stations": {"place-kencl": 300, "place-pktrm": 500}}]


def test_station_ridership_merges_renamed_stations_and_skips_unknown_ones():
    path = _write_station_csv(
        [
            ("2026-09-14", "Charles MGH", 40),
            ("2026-09-14", "Charles/MGH", 60),
            ("2026-09-14", "10 Park Plaza", 5),
            ("2026-09-14", "World Trade Center", 900),
        ]
    )
    assert format_subway_station_data(path) == {
        "line-Red": [{"date": "2026-09-14", "stations": {"place-chmnl": 100}}],
    }


def test_week_without_a_monday_row_keeps_its_iso_monday_label():
    """Labels used to come from a row dated Monday, so a week with none (e.g. no holiday service) was dropped."""
    rows = [(d, 100) for d in ("2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11")]  # Tue-Fri after Labor Day
    assert _ride_weeks(rows) == {"2026-09-07": 100}


def test_ferry_lines_missing_from_gtfs_are_rebuilt_from_route_data_or_skipped():
    new_by_line = {"line-Boat-F1": [{"date": "2026-09-07", "count": 1}]}
    by_route = {"Boat-F1": [], "Boat-F9": [{"date": "2025-09-08", "count": 5}, {"date": "2025-09-01", "count": 4}]}
    skipped = add_lines_missing_from_gtfs(new_by_line, {"line-Boat-F1", "line-Boat-F9", "line-Boat-Old"}, by_route)
    assert skipped == ["line-Boat-Old"]
    assert new_by_line["line-Boat-F9"] == [{"date": "2025-09-01", "count": 4}, {"date": "2025-09-08", "count": 5}]
    assert "line-Boat-Old" not in new_by_line
