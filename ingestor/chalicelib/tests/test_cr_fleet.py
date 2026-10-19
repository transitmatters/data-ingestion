from datetime import date
from decimal import Decimal

from .. import cr_fleet
from ..cr_fleet import CR_FLEET_STOPS, compute_cr_metrics, get_cr_info


def test_cr_info_range_edges():
    assert get_cr_info(1827) == (2013, "rotem")
    assert get_cr_info(1828) == (2023, "rotem")
    assert get_cr_info(1712) == (1990.5, "kawasaki")
    assert get_cr_info(1776) == (1974, "locomotive")
    assert get_cr_info(1003) == (None, "locomotive")
    assert get_cr_info(1405) == (None, "rotem")
    assert get_cr_info(9999) is None


def test_metrics_mix_and_cab_car_age():
    # 2 trips on the same Rotem cab car, 1 Kawasaki, 1 locomotive-led, 1 unknown
    metrics = compute_cr_metrics([1830, 1830, 1705, 1057, 9999], date(2026, 9, 24))
    assert metrics["fleet_trips"] == 4
    assert metrics["fleet_mix_rotem"] == Decimal("50.0")
    assert metrics["fleet_mix_kawasaki"] == Decimal("25.0")
    assert metrics["fleet_mix_locomotive"] == Decimal("25.0")
    assert abs(sum(v for k, v in metrics.items() if k.startswith("fleet_mix_")) - 100) <= Decimal("0.2")
    # Cab cars only, unique: 1830 (2023) and 1705 (1990.5) against 2026.5
    assert metrics["avg_cab_car_age"] == Decimal("19.8")


def test_no_known_vehicles():
    assert compute_cr_metrics([], date(2026, 9, 24)) == {}
    assert compute_cr_metrics([9999], date(2026, 9, 24)) == {}


def test_rows_keyed_by_route_plus_system_wide(monkeypatch):
    vehicles = {"CR-Fairmount": [1812, 1848], "CR-Worcester": [1705]}
    monkeypatch.setattr(cr_fleet, "CR_FLEET_STOPS", {route: [] for route in vehicles})
    monkeypatch.setattr(cr_fleet, "_vehicle_ids_for_route", lambda route, _: vehicles[route])

    rows = {row["route"]: row for row in cr_fleet.get_cr_fleet_metrics(date(2026, 9, 24))}

    assert set(rows) == {"CR-Fairmount", "CR-Worcester", "all"}
    assert rows["all"]["fleet_trips"] == 3


def test_stops_match_their_route():
    for route, stops in CR_FLEET_STOPS.items():
        assert stops and all(stop.split("_")[0] == route for stop in stops)
