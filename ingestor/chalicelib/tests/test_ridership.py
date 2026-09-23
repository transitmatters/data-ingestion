from tempfile import NamedTemporaryFile

import pandas as pd

from ..ridership.ingest import get_ridership_by_line_id
from ..ridership.process import pre_process_csv


def _write_daily_csv(rows):
    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(rows, columns=["TripDate", "CompletedTrips"]).to_csv(path, index=False)
    return path


def test_pre_process_csv_labels_weeks_with_iso_monday():
    path = _write_daily_csv(
        [
            # Week of Mon 2026-08-24
            ("2026/08/24 04:00:00+00", 1),
            ("2026/08/30 04:00:00+00", 2),
            # Week of Mon 2026-08-31
            ("2026/08/31 04:00:00+00", 10),
        ]
    )
    df = pd.read_csv(pre_process_csv(path, "TripDate", None, "CompletedTrips", route_name="RIDE"))
    assert dict(zip(df["TripDate"], df["CompletedTrips"])) == {"2026-08-24": 3, "2026-08-31": 10}


def test_pre_process_csv_handles_iso_year_boundary():
    # Mon 2025-12-29 through Sun 2026-01-04 is ISO week 1 of 2026
    path = _write_daily_csv([("2025-12-29", 1), ("2025-12-31", 2), ("2026-01-02", 4)])
    df = pd.read_csv(pre_process_csv(path, "TripDate", None, "CompletedTrips", route_name="RIDE"))
    assert dict(zip(df["TripDate"], df["CompletedTrips"])) == {"2025-12-29": 7}


def test_silver_line_gated_data_not_counted_as_bus():
    ridership_by_route_id = {
        "741": [{"date": "2026-09-07", "count": 100}],
        "1": [{"date": "2026-09-07", "count": 50}],
        "Silver Line": [{"date": "2026-09-07", "count": 90}, {"date": "2026-09-14", "count": 95}],
    }
    by_line_id = get_ridership_by_line_id(ridership_by_route_id, {})
    assert by_line_id["line-bus"] == [{"date": "2026-09-07", "count": 150}]
