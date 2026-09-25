from datetime import date
from decimal import Decimal

from ..bus_fleet import BUS_FLEET_STOPS, compute_bus_metrics, get_bus_info


def test_bus_info_range_edges():
    assert get_bus_info(840) == (2008, "diesel")
    assert get_bus_info(1294) == (2018, "hybrid")
    assert get_bus_info(1295) == (2019, "battery")
    assert get_bus_info(1700) == (2016.5, "cng")
    assert get_bus_info(4305) == (None, "battery")
    assert get_bus_info(620) is None  # work bus, not in revenue service


def test_metrics_mix_and_battery_share():
    # 2 battery trips (same bus), 1 hybrid, 1 CNG, 1 unknown bus
    metrics = compute_bus_metrics([4201, 4201, 1900, 1700, 9999], date(2026, 9, 22))
    assert metrics["trips"] == 4
    assert metrics["pct_battery_trips"] == Decimal("50.0")
    assert metrics["fleet_mix_battery"] == Decimal("50.0")
    assert metrics["fleet_mix_diesel"] == 0
    assert abs(sum(v for k, v in metrics.items() if k.startswith("fleet_mix_")) - 100) <= Decimal("0.2")
    # Unique buses only: 4201 (2025), 1900 (2016.5), 1700 (2016.5) against 2026.5
    assert metrics["avg_bus_age"] == Decimal("7.2")


def test_on_order_buses_count_as_battery_without_age():
    metrics = compute_bus_metrics([4310], date(2026, 9, 22))
    assert metrics["fleet_mix_battery"] == 100
    assert "avg_bus_age" not in metrics


def test_no_known_buses():
    assert compute_bus_metrics([], date(2026, 9, 22)) == {}
    assert compute_bus_metrics([9999], date(2026, 9, 22)) == {}


def test_stops_match_their_route():
    for route, stops in BUS_FLEET_STOPS.items():
        assert stops and all(stop.split("-")[0] == route for stop in stops)
