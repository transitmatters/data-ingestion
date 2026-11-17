"""One bad ridership source must not stop the update for every mode (#264)."""

import re
from tempfile import NamedTemporaryFile
from unittest import mock

import pandas as pd
import requests

from ..ridership import arcgis
from ..ridership.process import format_ridership_csv, format_subway_data, unofficial_cr_labels_map
from ..ridership.sharepoint import SharepointConnection


def _csv(rows, columns):
    path = NamedTemporaryFile(suffix=".csv", delete=False).name
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)
    return path


def test_unknown_label_is_skipped_not_fatal():
    known = next(iter(unofficial_cr_labels_map))
    path = _csv(
        [("2026-09-14", known, 100), ("2026-09-14", "Foxboro", 50)],
        ["servicedate", "line", "estimated_boardings"],
    )
    by_route = format_ridership_csv(path, "servicedate", "line", "estimated_boardings", unofficial_cr_labels_map)
    assert list(by_route) == [unofficial_cr_labels_map[known]]


def test_subway_file_starting_mid_week_drops_the_dateless_week():
    path = _csv(
        [
            ("2026-09-09", "Red Line", 10),  # Wednesday: its week has no Monday row
            ("2026-09-14", "Red Line", 20),
            ("2026-09-15", "Red Line", 30),
        ],
        ["servicedate", "route_or_line", "validations"],
    )
    entries = next(iter(format_subway_data(path).values()))
    assert entries == [{"date": "2026-09-14", "count": 25}]


def test_failed_sharepoint_download_returns_none():
    connection = SharepointConnection()
    files = [{"name": "MBTA Bus Weekly Ridership.xlsx", "url": "/x", "is_folder": False, "size": 1}]
    with (
        mock.patch.object(connection, "get_sharepoint_folder_contents_anonymous", return_value=files),
        mock.patch.object(connection, "download_sharepoint_file_anonymous", return_value=False),
    ):
        assert connection.fetch_sharepoint_file(file_regex=re.compile(r"Bus"), bus_data=True) is None


class _Response:
    def __init__(self, status, content=b"a,b\n1,2\n"):
        self.status_code = status
        self.content = content

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError(str(self.status_code))


def test_arcgis_error_skips_that_mode(monkeypatch):
    monkeypatch.setattr(arcgis.requests, "get", lambda url, **kwargs: _Response(500, b'{"error": "x"}'))
    assert arcgis._download_arcgis_csv("https://example.invalid", "CR") is None


def test_arcgis_success_writes_the_file(monkeypatch):
    monkeypatch.setattr(arcgis.requests, "get", lambda url, **kwargs: _Response(200))
    path = arcgis._download_arcgis_csv("https://example.invalid", "CR")
    with open(path, "rb") as f:
        assert f.read() == b"a,b\n1,2\n"


def test_cache_refresh_failure_is_not_fatal(monkeypatch):
    def down(url, **kwargs):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(arcgis.requests, "get", down)
    arcgis.cr_update_cache()
