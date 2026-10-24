from datetime import date

from .. import shutdowns

DAY = date(2026, 10, 8)

SHUTTLE = "Shuttle buses replace Red Line service between Alewife and Harvard"


def _alert(alert_id, periods, effect="SHUTTLE", closed=None, entities=None, header=SHUTTLE):
    return {
        "id": alert_id,
        "type": "alert",
        "attributes": {
            "effect": effect,
            "cause": "MAINTENANCE",
            "header": header,
            "closed_timestamp": closed,
            "active_period": [{"start": start, "end": end} for start, end in periods],
            "informed_entity": entities
            or [
                {"route": "Red", "stop": "place-alfcl"},
                {"route": "Red", "stop": "place-harsq"},
            ],
        },
    }


def test_multi_day_shuttle():
    alert = _alert("1", [("2026-10-03T03:00:00-04:00", "2026-10-06T03:00:00-04:00")])
    [item] = shutdowns.shutdown_items(alert, DAY)
    assert item == {
        "line": "Red",
        "alert_id": "1",
        "effect": "SHUTTLE",
        "cause": "MAINTENANCE",
        "header": SHUTTLE,
        "stops": ["place-alfcl", "place-harsq"],
        "periods": [{"start": "2026-10-03T03:00:00-04:00", "end": "2026-10-06T03:00:00-04:00"}],
        "first_start": "2026-10-03",
        "last_end": "2026-10-05",
    }


def test_incident_shuttle_is_dropped():
    alert = _alert("1", [("2026-10-08T08:00:00-04:00", "2026-10-08T08:40:00-04:00")])
    assert shutdowns.shutdown_items(alert, DAY) == []


def test_other_effects_are_dropped():
    alert = _alert("1", [("2026-10-03T03:00:00-04:00", "2026-10-06T03:00:00-04:00")], effect="DELAY")
    assert shutdowns.shutdown_items(alert, DAY) == []


def test_closed_early_is_clipped():
    alert = _alert(
        "1",
        [
            ("2026-10-03T03:00:00-04:00", "2026-10-06T03:00:00-04:00"),
            # Cancelled: it would have started after the alert was taken down
            ("2026-10-10T03:00:00-04:00", "2026-10-13T03:00:00-04:00"),
        ],
        closed="2026-10-04T15:00:00-04:00",
    )
    [item] = shutdowns.shutdown_items(alert, DAY)
    assert item["periods"] == [{"start": "2026-10-03T03:00:00-04:00", "end": "2026-10-04T15:00:00-04:00"}]
    assert item["last_end"] == "2026-10-04"


def test_ongoing_runs_through_last_day_seen():
    alert = _alert("1", [("2026-10-01T03:00:00-04:00", None)], effect="STATION_CLOSURE")
    [item] = shutdowns.shutdown_items(alert, DAY)
    assert item["periods"][0]["end"] == "2026-10-09T03:00:00-04:00"
    assert item["last_end"] == "2026-10-08"


def test_evening_shutdown_belongs_to_its_service_day():
    alert = _alert("1", [("2026-10-06T20:45:00-04:00", "2026-10-07T02:30:00-04:00")])
    [item] = shutdowns.shutdown_items(alert, DAY)
    assert (item["first_start"], item["last_end"]) == ("2026-10-06", "2026-10-06")


def test_one_item_per_line():
    alert = _alert(
        "1",
        [("2026-10-03T03:00:00-04:00", "2026-10-06T03:00:00-04:00")],
        entities=[
            {"route": "Green-B", "stop": "place-kencl"},
            {"route": "Green-C", "stop": "place-kencl"},
            {"route": "Green-D", "stop": "place-kencl"},
            {"route": "Shuttle-Generic", "stop": "place-kencl"},
            {"route_type": 3},
        ],
    )
    items = shutdowns.shutdown_items(alert, DAY)
    assert sorted(item["line"] for item in items) == ["Green-B", "Green-C", "Green-D"]
    assert all(item["stops"] == ["place-kencl"] for item in items)


def test_latest_day_file_wins(monkeypatch):
    first = _alert("1", [("2026-10-03T03:00:00-04:00", "2026-10-06T03:00:00-04:00")])
    revised = _alert("1", [("2026-10-03T03:00:00-04:00", "2026-10-07T03:00:00-04:00")])
    files = {date(2026, 10, 3): {"1": first}, date(2026, 10, 5): {"1": revised}}
    monkeypatch.setattr(shutdowns, "load_lamp_alerts", lambda day: files.get(day))

    [item] = shutdowns.collect_shutdowns(date(2026, 10, 3), date(2026, 10, 6))
    assert item["last_end"] == "2026-10-06"
