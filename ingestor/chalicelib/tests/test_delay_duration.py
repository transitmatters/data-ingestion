from datetime import date

from .. import constants
from ..delays import duration, process

DAY = date(2026, 10, 8)


def _alert(alert_id, header, start, end=None, closed=None, route="Red", effect="DELAY", created=None):
    return {
        alert_id: {
            "id": alert_id,
            "type": "alert",
            "attributes": {
                "effect": effect,
                "header": header,
                "created_at": created or start,
                "closed_timestamp": closed,
                "active_period": [{"start": start, "end": end}],
                "informed_entity": [{"route": route, "route_type": 1, "activities": ["BOARD", "EXIT", "RIDE"]}],
            },
        }
    }


DISABLED = "Red Line: Delays of about 15 minutes due to a disabled train at Harvard."


def _minutes(alerts, line="Red"):
    return duration.union_minutes(duration.live_intervals(alerts, line, DAY, process.alert_is_delay))


def test_closed_timestamp_beats_placeholder_end():
    # Subway delay alerts are posted with a ~2h end; the real close was 37 minutes in
    alerts = _alert(
        "1", DISABLED, "2026-10-08T08:00:00-04:00", end="2026-10-08T10:00:00-04:00", closed="2026-10-08T08:37:00-04:00"
    )
    assert _minutes(alerts) == 37


def test_period_end_beats_late_close():
    # The v3 API stops showing an alert at its period end even if it's closed later
    alerts = _alert(
        "1", DISABLED, "2026-10-08T15:00:00-04:00", end="2026-10-08T15:30:00-04:00", closed="2026-10-08T17:00:00-04:00"
    )
    assert _minutes(alerts) == 30


def test_live_from_when_posted_not_period_start():
    alerts = _alert(
        "1",
        DISABLED,
        "2026-10-08T08:00:00-04:00",
        closed="2026-10-08T09:00:00-04:00",
        created="2026-10-08T08:20:00-04:00",
    )
    assert _minutes(alerts) == 40


def test_forgotten_alert_is_capped():
    alerts = _alert("1", DISABLED, "2026-10-08T06:00:00-04:00", closed="2026-10-09T02:00:00-04:00")
    assert _minutes(alerts) == 3 * 60


def test_clipped_to_service_day():
    # Runs 1am-4am on the 8th: 1am-3am is the 7th's service day, 3am-4am the 8th's
    alerts = _alert("1", DISABLED, "2026-10-08T01:00:00-04:00", closed="2026-10-08T04:00:00-04:00")
    assert _minutes(alerts) == 60
    assert (
        duration.union_minutes(duration.live_intervals(alerts, "Red", date(2026, 10, 7), process.alert_is_delay)) == 120
    )


def test_overlapping_alerts_are_not_double_counted():
    alerts = {
        **_alert("1", DISABLED, "2026-10-08T08:00:00-04:00", closed="2026-10-08T09:00:00-04:00"),
        **_alert("2", DISABLED, "2026-10-08T08:30:00-04:00", closed="2026-10-08T09:30:00-04:00"),
        **_alert("3", DISABLED, "2026-10-08T12:00:00-04:00", closed="2026-10-08T12:10:00-04:00"),
    }
    assert _minutes(alerts) == 100


def test_only_counts_alerts_the_text_parser_counts():
    alerts = {
        **_alert(
            "1",
            "Red Line: Shuttle buses replace service.",
            "2026-10-08T08:00:00-04:00",
            closed="2026-10-08T09:00:00-04:00",
        ),
        **_alert(
            "2", DISABLED, "2026-10-08T08:00:00-04:00", closed="2026-10-08T09:00:00-04:00", effect="ELEVATOR_CLOSURE"
        ),
        **_alert("3", DISABLED, "2026-10-08T08:00:00-04:00", closed="2026-10-08T09:00:00-04:00", route="Orange"),
        **_alert("4", DISABLED, "2026-10-08T08:00:00-04:00"),  # no end or close: skipped, not guessed
    }
    assert _minutes(alerts) == 0
    assert _minutes(alerts, line="Orange") == 60


def test_process_requests_keeps_stated_minutes_next_to_live_minutes(monkeypatch):
    lamp = _alert("1", DISABLED, "2026-10-08T08:00:00-04:00", closed="2026-10-08T08:37:00-04:00")
    monkeypatch.setattr(
        process,
        "process_single_day",
        lambda request: [{"valid_from": "", "valid_to": "", "text": DISABLED}],
    )
    monkeypatch.setattr(duration, "load_lamp_alerts", lambda day: lamp if day == DAY else None)

    requests = process.generate_requests(DAY, date(2026, 10, 9), ["Red"])
    df = process.process_requests(requests, ["Red"])["Red"]
    records = {r["date"]: r for r in process.group_daily_data(df, DAY.isoformat())}

    assert records["2026-10-08"]["total_delay_time"] == 15
    assert records["2026-10-08"]["delay_alert_minutes"] == 37
    assert records["2026-10-08"]["delay_alert_minutes_disabled_vehicle"] == 37
    # No LAMP file yet (today): stated minutes still written, live minutes left out rather than zero
    assert records["2026-10-09"]["total_delay_time"] == 15
    assert not any(k.startswith("delay_alert_minutes") for k in records["2026-10-09"])
    assert set(constants.DELAY_BY_TYPE) <= set(records["2026-10-09"])
