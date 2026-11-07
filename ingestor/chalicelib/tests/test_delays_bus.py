from datetime import date, timedelta

from ..delays import bus, duration, process

DAY = date(2026, 10, 20)


def _alert(alert_id, header, routes, route_type=3, effect="DELAY", start="2026-10-20T07:30:00-04:00", closed=None):
    return {
        alert_id: {
            "id": alert_id,
            "type": "alert",
            "attributes": {
                "effect": effect,
                "header": header,
                "short_header": None,
                "created_at": start,
                "closed_timestamp": closed,
                "active_period": [{"start": start, "end": "2026-10-20T09:30:00-04:00"}],
                "informed_entity": [{"route": route, "route_type": route_type, "activities": []} for route in routes],
            },
        }
    }


SL = "741,742,743,746"

LAMP = {
    **_alert(
        "1",
        "Route 39 is experiencing delays of about 20 minutes due to traffic.",
        ["39"],
        closed="2026-10-20T08:10:00-04:00",
    ),
    **_alert("2", "SL1 is experiencing delays of about 25 minutes due to an accident.", ["741", "742"]),
    **_alert("3", "Red Line: Delays of about 10 minutes due to a signal problem.", ["Red"], route_type=1),
    **_alert("4", "Route 1 is detoured due to construction.", ["1"], effect="DETOUR"),
    **_alert("5", "Route 66 shuttle buses replace service.", ["66"], effect="SHUTTLE"),
}


# --- bus_alerts_by_route ---


def test_bus_alerts_by_route_keeps_bus_routes_only():
    by_route = bus.bus_alerts_by_route(LAMP)
    assert set(by_route) == {"39", "741", "742", "1", SL}
    assert by_route["39"][0]["text"].startswith("Route 39")
    assert by_route["39"][0]["valid_from"] == "2026-10-20T07:30:00-04:00"


def test_bus_alerts_by_route_one_row_per_active_period():
    alerts = _alert("1", "Route 39 is experiencing delays of about 20 minutes due to traffic.", ["39"])
    alerts["1"]["attributes"]["active_period"].append(
        {"start": "2026-10-20T16:00:00-04:00", "end": "2026-10-20T18:00:00-04:00"}
    )
    assert len(bus.bus_alerts_by_route(alerts)["39"]) == 2


# --- process_bus_days ---


def _bus_days(monkeypatch, files, start=DAY, end=DAY):
    monkeypatch.setattr(duration, "load_lamp_alerts", lambda day: files.get(day))
    return process.process_bus_days(start, end)


def test_process_bus_days_writes_only_routes_with_delays(monkeypatch):
    dfs = _bus_days(monkeypatch, {DAY: LAMP})
    # Route 1 only has a detour, so it gets no row
    assert set(dfs) == {"39", "741", "742", SL, bus.BUS_AGGREGATE}

    route_39 = dfs["39"].iloc[0]
    assert route_39["total_delay_time"] == 20
    assert route_39["car_traffic"] == 20
    assert route_39["delay_alert_minutes"] == 40
    assert route_39["delay_alert_minutes_car_traffic"] == 40

    # The accident is car traffic because it's a bus alert
    assert dfs["741"].iloc[0]["car_traffic"] == 25


def test_process_bus_days_counts_each_alert_once_in_bus(monkeypatch):
    total = _bus_days(monkeypatch, {DAY: LAMP})[bus.BUS_AGGREGATE].iloc[0]
    assert total["line"] == bus.BUS_AGGREGATE
    # SL1's alert also covers SL2, but counts once
    assert total["total_delay_time"] == 20 + 25
    assert total["car_traffic"] == 45
    # Route 39's and SL1's alerts overlap in time, but are on different routes, so both count
    assert total["delay_alert_minutes"] == 40 + 120
    assert total["delay_alert_minutes_car_traffic"] == 160


def test_process_bus_days_group_counts_shared_alert_once(monkeypatch):
    lamp = {
        **_alert("1", "SL1 is experiencing delays of about 25 minutes due to traffic.", ["741", "742", "746"]),
        **_alert(
            "2",
            "SLW is experiencing delays of about 10 minutes due to traffic.",
            ["746"],
            start="2026-10-20T09:00:00-04:00",
        ),
    }
    lamp["2"]["attributes"]["active_period"][0]["end"] = "2026-10-20T10:00:00-04:00"
    dfs = _bus_days(monkeypatch, {DAY: lamp})

    # Single routes still count every alert posted on them
    assert dfs["741"].iloc[0]["total_delay_time"] == 25
    assert dfs["746"].iloc[0]["total_delay_time"] == 25 + 10
    assert dfs["746"].iloc[0]["delay_alert_minutes"] == 150  # 7:30 to 10:00

    group = dfs[SL].iloc[0]
    assert group["line"] == SL
    assert group["total_delay_time"] == 25 + 10
    assert group["car_traffic"] == 35
    # The union of 7:30-9:30 and 9:00-10:00, not 120 + 120 + 150
    assert group["delay_alert_minutes"] == 150
    assert group["delay_alert_minutes_car_traffic"] == 150

    assert dfs[bus.BUS_AGGREGATE].iloc[0]["total_delay_time"] == 35


def test_process_bus_days_no_group_row_without_delays(monkeypatch):
    dfs = _bus_days(monkeypatch, {DAY: _alert("1", "Route 114 is detoured.", ["114"], effect="DETOUR")})
    assert "114,116,117" not in dfs


def test_process_bus_days_quiet_day_still_writes_bus(monkeypatch):
    dfs = _bus_days(monkeypatch, {DAY: _alert("4", "Route 1 is detoured.", ["1"], effect="DETOUR")})
    assert set(dfs) == {bus.BUS_AGGREGATE}
    assert dfs[bus.BUS_AGGREGATE].iloc[0]["total_delay_time"] == 0


def test_process_bus_days_skips_days_without_lamp_file(monkeypatch):
    dfs = _bus_days(monkeypatch, {DAY: LAMP}, end=DAY + timedelta(days=1))
    assert list(dfs[bus.BUS_AGGREGATE].index) == [process.pd.Timestamp(DAY)]


def test_update_table_writes_bus_daily_rows(monkeypatch):
    monkeypatch.setattr(duration, "load_lamp_alerts", lambda day: LAMP)
    written = []
    monkeypatch.setattr(process.dynamo, "dynamo_batch_write", lambda items, table: written.extend(items))
    process.update_table(DAY, DAY, lines=[])
    rows = {r["line"]: r for r in written}
    assert set(rows) == {"39", "741", "742", SL, bus.BUS_AGGREGATE}
    assert rows["39"]["date"] == DAY.isoformat()
    assert rows[SL]["total_delay_time"] == 25
    assert rows[bus.BUS_AGGREGATE]["total_delay_time"] == 45


def test_update_weekly_bus_only(monkeypatch):
    # Monday to Sunday; LAMP has the same alerts every day
    monday = date(2026, 10, 19)
    monkeypatch.setattr(duration, "load_lamp_alerts", lambda day: LAMP)
    monkeypatch.setattr(process.dynamo, "query_dynamo", lambda params, table: [])
    written = []
    monkeypatch.setattr(process.dynamo, "dynamo_batch_write", lambda items, table: written.extend(items))
    process.update_weekly_from_daily(monday, monday + timedelta(days=6), lines=[])
    rows = {(r["line"], r["date"]): r for r in written}
    assert rows[("39", monday.isoformat())]["total_delay_time"] == 7 * 20
    assert rows[(SL, monday.isoformat())]["total_delay_time"] == 7 * 25
    assert rows[(bus.BUS_AGGREGATE, monday.isoformat())]["total_delay_time"] == 7 * 45
