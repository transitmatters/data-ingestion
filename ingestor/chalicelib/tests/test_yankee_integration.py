"""
Integration tests for yankee.py using the in-memory GTFS session fixture
defined in conftest.py.

These tests call the real SQLAlchemy query functions (get_shuttle_stops,
get_shuttle_shapes) against a seeded in-memory SQLite database — no AWS,
no network, no Samsara API required.
"""

from ..yankee import get_shuttle_shapes, get_shuttle_stops


# ---------------------------------------------------------------------------
# get_shuttle_stops
# ---------------------------------------------------------------------------


def test_get_shuttle_stops_returns_only_shuttle_stops(gtfs_session):
    stops = get_shuttle_stops(gtfs_session)
    assert len(stops) == 2
    assert all("Shuttle" in s.platform_name for s in stops)


def test_get_shuttle_stops_excludes_non_shuttle_stop(gtfs_session):
    stops = get_shuttle_stops(gtfs_session)
    stop_ids = {s.stop_id for s in stops}
    assert "stop-3" not in stop_ids


def test_get_shuttle_stops_includes_expected_ids(gtfs_session):
    stops = get_shuttle_stops(gtfs_session)
    stop_ids = {s.stop_id for s in stops}
    assert stop_ids == {"stop-1", "stop-2"}


def test_get_shuttle_stops_have_coordinates(gtfs_session):
    stops = get_shuttle_stops(gtfs_session)
    for stop in stops:
        assert stop.stop_lat is not None
        assert stop.stop_lon is not None


# ---------------------------------------------------------------------------
# get_shuttle_shapes
# ---------------------------------------------------------------------------


def test_get_shuttle_shapes_returns_diversion_routes_only(gtfs_session):
    # Only Shuttle-AlewifeParkSt has DIVERSION typicality AND a trip seeded.
    # Red-1-0 has TYPICAL typicality → filtered out by the query.
    shapes = get_shuttle_shapes(gtfs_session)
    assert "Shuttle-AlewifeParkSt" in shapes


def test_get_shuttle_shapes_excludes_non_diversion(gtfs_session):
    shapes = get_shuttle_shapes(gtfs_session)
    assert "Red" not in shapes


def test_get_shuttle_shapes_has_correct_point_count(gtfs_session):
    shapes = get_shuttle_shapes(gtfs_session)
    assert len(shapes["Shuttle-AlewifeParkSt"]) == 4


def test_get_shuttle_shapes_points_ordered_by_sequence(gtfs_session):
    shapes = get_shuttle_shapes(gtfs_session)
    points = shapes["Shuttle-AlewifeParkSt"]
    sequences = [p.shape_pt_sequence for p in points]
    assert sequences == sorted(sequences)


def test_get_shuttle_shapes_points_have_coordinates(gtfs_session):
    shapes = get_shuttle_shapes(gtfs_session)
    for point in shapes["Shuttle-AlewifeParkSt"]:
        assert point.shape_pt_lat is not None
        assert point.shape_pt_lon is not None


def test_get_shuttle_shapes_skips_pattern_with_no_trip(gtfs_session):
    # "Red-1-0" is a DIVERSION-like pattern but its representative trip
    # "trip-red-1" was intentionally not seeded, so get_shuttle_shapes should
    # skip it gracefully rather than raising.
    # Re-add a DIVERSION pattern pointing to a missing trip to confirm skip.
    from .conftest import make_route_pattern
    from mbta_gtfs_sqlite.models import RoutePatternTypicality

    gtfs_session.add(
        make_route_pattern(
            route_pattern_id="Shuttle-Missing-0-0",
            route_id="Shuttle-Missing",
            representative_trip_id="trip-does-not-exist",
            typicality=RoutePatternTypicality.DIVERSION,
        )
    )
    gtfs_session.commit()

    shapes = get_shuttle_shapes(gtfs_session)
    # The pattern with the missing trip should be skipped, not raise
    assert "Shuttle-Missing" not in shapes
    # The valid pattern is still present
    assert "Shuttle-AlewifeParkSt" in shapes
