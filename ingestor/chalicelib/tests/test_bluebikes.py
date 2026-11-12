import io
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from .. import bluebikes

INGESTOR_DIR = Path(__file__).resolve().parents[2]

STATIONS = [
    {
        "station_id": "a1",
        "num_bikes_available": 5,
        "num_docks_available": 10,
        "is_installed": 1,
        "is_renting": True,
        "last_reported": 1793836200,
        "vehicle_types_available": [{"vehicle_type_id": "1", "count": 4}],
    },
    # Missing num_bikes_disabled-style keys and vehicle_types_available on purpose
    {
        "station_id": "b2",
        "num_bikes_available": 0,
        "num_docks_available": 19,
        "is_installed": 0,
        "is_renting": False,
        "last_reported": 1793836100,
    },
    # A key the first station doesn't have, to check first-seen column ordering
    {
        "station_id": "c3",
        "num_bikes_available": 3,
        "num_docks_available": 7,
        "is_installed": 1,
        "is_renting": True,
        "last_reported": 1793836000,
        "num_ebikes_available": 2,
    },
]
TIMESTAMP = 1793836210


def _pandas_csv(stations, timestamp):
    """The pre-rewrite serialization, kept as the reference output."""
    df = pd.DataFrame.from_records(stations)
    df["datetimepulled"] = timestamp
    return df.to_csv(index=False)


def test_station_status_csv_matches_pandas():
    ours = pd.read_csv(io.StringIO(bluebikes.station_status_to_csv(STATIONS, TIMESTAMP)))
    reference = pd.read_csv(io.StringIO(_pandas_csv(STATIONS, TIMESTAMP)))
    pd.testing.assert_frame_equal(ours, reference)


def test_station_status_csv_column_order():
    header = bluebikes.station_status_to_csv(STATIONS, TIMESTAMP).splitlines()[0]
    assert header == _pandas_csv(STATIONS, TIMESTAMP).splitlines()[0]
    assert header.split(",")[-1] == "datetimepulled"


def test_store_station_status_uploads_csv(monkeypatch):
    uploads = []
    monkeypatch.setattr(
        bluebikes,
        "get_station_status",
        lambda: {"last_updated": TIMESTAMP, "data": {"stations": STATIONS}},
    )
    monkeypatch.setattr(bluebikes.s3, "upload_csv", lambda bucket, key, body: uploads.append((bucket, key, body)))

    bluebikes.store_station_status()

    assert len(uploads) == 1
    bucket, key, body = uploads[0]
    assert bucket == "tm-bluebikes"
    # 1793836210 is 2026-11-04 in US/Eastern
    assert key == f"station_status/2026-11-04/{TIMESTAMP}/bluebikes.csv"
    assert len(pd.read_csv(io.StringIO(body))) == len(STATIONS)


def test_gather_single_day_data_keeps_key_order(monkeypatch):
    keys = [f"station_status/2026-11-04/{ts}/bluebikes.csv" for ts in range(40)]
    monkeypatch.setattr(bluebikes.s3, "ls", lambda bucket, prefix: keys)
    monkeypatch.setattr(
        bluebikes.s3,
        "download_csv_as_df",
        lambda bucket, key: pd.DataFrame({"datetimepulled": [int(key.split("/")[2])]}),
    )

    df = bluebikes.gather_single_day_data("2026-11-04")

    assert df["datetimepulled"].tolist() == list(range(40))


def test_gather_single_day_data_fails_clearly_without_snapshots(monkeypatch):
    monkeypatch.setattr(bluebikes.s3, "ls", lambda bucket, prefix: [])

    with pytest.raises(ValueError, match="No Bluebikes station_status snapshots .* for 2026-11-04"):
        bluebikes.gather_single_day_data("2026-11-04")


def test_status_poller_does_not_import_pandas():
    """Importing app.py and running the 5-minute poller's module must not load pandas.

    That job runs every 5 minutes on a small memory size; pandas and numpy push it to the limit.
    """
    code = (
        "import sys, app; from chalicelib import bluebikes; "
        "heavy = [m for m in ('pandas', 'numpy') if m in sys.modules]; "
        "sys.exit(f'loaded {heavy}' if heavy else 0)"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=INGESTOR_DIR, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout
