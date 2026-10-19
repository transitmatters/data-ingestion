from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from mbta_gtfs_sqlite.models import ServiceDayAvailability

from ..gtfs.models import SessionModels
from ..gtfs.regional_rail import (
    HOUR,
    clock_face_share,
    compute_regional_rail_metrics,
    create_regional_rail_items,
    hours_at_target,
    max_gap,
)

MINUTE = 60


def trip(direction_id: str, start: int, end: int, service_id: str = "weekday"):
    return SimpleNamespace(direction_id=direction_id, start_time=start, end_time=end, service_id=service_id)


def every(minutes: int, first_hour: int, last_hour: int, offset: int = 0):
    return list(range(first_hour * HOUR + offset * MINUTE, last_hour * HOUR + 1, minutes * MINUTE))


def test_clock_face_schedule_scores_one():
    assert clock_face_share(every(60, 5, 24, offset=10)) == Decimal(1)
    assert clock_face_share(every(30, 5, 24, offset=10)) == Decimal(1)


def test_two_hourly_and_irregular_schedules_are_not_clock_face():
    assert clock_face_share(every(120, 6, 22)) == Decimal(0)
    irregular = [6 * HOUR, 6 * HOUR + 47 * MINUTE, 8 * HOUR + 5 * MINUTE, 9 * HOUR + 50 * MINUTE]
    assert clock_face_share(irregular) == Decimal(0)
    assert clock_face_share([8 * HOUR]) is None


def test_midday_gap_spans_the_trains_either_side_of_the_window():
    times = [7 * HOUR, 8 * HOUR, 16 * HOUR, 17 * HOUR]
    assert max_gap(times, 10 * HOUR, 15 * HOUR) == 8 * HOUR


def test_three_hour_midday_gap():
    times = [9 * HOUR, 10 * HOUR, 11 * HOUR, 14 * HOUR, 15 * HOUR]
    assert max_gap(times, 10 * HOUR, 15 * HOUR) == 3 * HOUR


def test_window_without_service_uses_window_edges():
    assert max_gap([], 19 * HOUR, 24 * HOUR) == 5 * HOUR
    # Last train at 9pm: the rest of the evening counts as one long wait
    assert max_gap([18 * HOUR, 21 * HOUR], 19 * HOUR, 24 * HOUR) == 3 * HOUR


def test_hours_at_target():
    assert hours_at_target(every(30, 4, 26)) == 20
    assert hours_at_target(every(15, 4, 26)) == 20
    assert hours_at_target(every(60, 4, 26)) == 0
    # Half-hourly 6am to 10am only: 6, 7, 8 and 9am
    assert hours_at_target(every(30, 6, 10)) == 4


def test_directions_are_measured_at_boston():
    trips = [
        trip("0", 8 * HOUR, 9 * HOUR),  # leaves South Station at 8
        trip("1", 7 * HOUR, 8 * HOUR),  # reaches South Station at 8
        trip("1", 16 * HOUR, 17 * HOUR),  # reaches South Station at 5pm: reverse peak
    ]
    metrics = compute_regional_rail_metrics(trips)
    assert metrics["outbound"]["firstTrip"] == 8 * HOUR
    assert metrics["inbound"]["firstTrip"] == 8 * HOUR
    assert metrics["inbound"]["lastTrip"] == 17 * HOUR
    assert metrics["inbound"]["byHour"][8] == 1
    assert metrics["peakTrips"] == 1  # the inbound arrival at 8am
    assert metrics["reversePeakTrips"] == 2  # outbound at 8am, inbound at 5pm


def test_no_service_day_is_reported():
    metrics = compute_regional_rail_metrics([])
    assert metrics["outbound"]["trips"] == 0
    assert metrics["outbound"]["firstTrip"] is None
    assert metrics["outbound"]["maxGapMidday"] == 5 * 60
    assert metrics["outbound"]["hoursAtTarget"] == 0


def _models(trips_by_route_id):
    weekday = SimpleNamespace(
        service_id="weekday",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        **{day: ServiceDayAvailability.AVAILABLE for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]},
        saturday=ServiceDayAvailability.NOT_AVAILABLE,
        sunday=ServiceDayAvailability.NOT_AVAILABLE,
    )
    return SessionModels(
        calendar_services={"weekday": weekday},
        calendar_attributes={},
        calendar_service_exceptions={},
        trips_by_route_id=trips_by_route_id,
        routes={
            route_id: SimpleNamespace(route_id=route_id, line_id=f"line-{route_id}") for route_id in trips_by_route_id
        },
    )


def test_items_cover_commuter_rail_routes_that_run_in_the_feed():
    models = _models(
        {
            "CR-Worcester": [trip("0", 8 * HOUR, 9 * HOUR)],
            "CR-Middleborough": [],
            "Red": [trip("0", 8 * HOUR, 9 * HOUR)],
        }
    )
    weekday_items = create_regional_rail_items(date(2026, 9, 30), models)
    assert [item["routeId"] for item in weekday_items] == ["CR-Worcester"]
    assert weekday_items[0]["outbound"]["trips"] == 1

    # A Saturday with no service is still written, so gaps in service show up
    saturday_items = create_regional_rail_items(date(2026, 10, 3), models)
    assert saturday_items[0]["outbound"]["trips"] == 0
    assert saturday_items[0]["date"] == "2026-10-03"
