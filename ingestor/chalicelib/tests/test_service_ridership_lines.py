from datetime import date
from unittest import mock

from ..service_ridership_dashboard import ingest
from ..service_ridership_dashboard.service_levels import ServiceLevelsEntry


def _service(line_id: str, days: int = 14) -> dict:
    start = date(2026, 10, 1)
    return {
        date.fromordinal(start.toordinal() + i): ServiceLevelsEntry(
            date=date.fromordinal(start.toordinal() + i),
            line_id=line_id,
            line_short_name=line_id,
            line_long_name=line_id,
            route_ids=[line_id.removeprefix("line-")],
            service_levels=[2] * 24,
            has_service_exceptions=False,
        )
        for i in range(days)
    }


def test_get_line_kind_mattapan_is_red_not_bus():
    assert ingest.get_line_kind(["Mattapan"], "line-Mattapan") == "red"


@mock.patch.object(ingest, "put_dashboard_json_to_s3")
@mock.patch.object(ingest, "get_routes_by_line", return_value={})
def test_mattapan_kept_without_ridership_and_left_out_of_totals(_routes, put_to_s3):
    """#281: lines with no ridership source stay in lineData with ridershipHistory None."""
    from ..service_ridership_dashboard.ridership import RidershipEntry

    service = {"line-Mattapan": _service("line-Mattapan"), "line-1": _service("line-1"), "line-9": _service("line-9")}
    ridership = {
        "line-Mattapan": {},
        "line-1": {date(2026, 10, 5): RidershipEntry(date=date(2026, 10, 5), ridership=100)},
        "line-9": {},  # a bus line with no ridership is still dropped
    }
    with (
        mock.patch.object(ingest, "get_service_level_entries_by_line_id", return_value=service),
        mock.patch.object(ingest, "ridership_by_line_id", return_value=ridership),
    ):
        ingest.create_service_ridership_dash_json(start_date=date(2026, 10, 1), end_date=date(2026, 10, 14))
    dash = put_to_s3.call_args.kwargs["dash_json"]

    assert set(dash["lineData"]) == {"line-Mattapan", "line-1"}
    assert dash["lineData"]["line-Mattapan"]["ridershipHistory"] is None
    assert dash["lineData"]["line-Mattapan"]["lineKind"] == "red"
    # Totals cover only line-1, as before
    assert dash["summaryData"]["totalTrips"] == 48
    assert set(dash["modeData"]) == {"bus"}
