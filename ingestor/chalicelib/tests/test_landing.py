from datetime import date

from .. import landing


def _week(line, d, count):
    return {"lineId": line, "date": d, "count": count, "timestamp": 1}


def test_window_holding_twelve_mondays_does_not_crash():
    """90 days ago through a week ago always holds 12 Mondays; an 11-slot buffer crashed on the 12th (#257)."""
    start, end = landing.landing_date_range(date(2026, 11, 17))
    assert (start, end) == ("2026-08-19", "2026-11-10")
    mondays = [
        f"2026-{m:02d}-{d:02d}"
        for m, d in [
            (8, 24),
            (8, 31),
            (9, 7),
            (9, 14),
            (9, 21),
            (9, 28),
            (10, 5),
            (10, 12),
            (10, 19),
            (10, 26),
            (11, 2),
            (11, 9),
        ]
    ]
    weeks = landing.combine_commuter_rail_weeks([[_week("line-Fairmount", d, 1) for d in mondays]])
    assert [w["date"] for w in weeks] == mondays


def test_lines_are_summed_by_date_not_position():
    fairmount = [_week("line-Fairmount", "2026-09-07", 10), _week("line-Fairmount", "2026-09-14", 20)]
    foxboro = [_week("line-Foxboro", "2026-09-14", 5)]  # missing the first week
    weeks = landing.combine_commuter_rail_weeks([foxboro, fairmount])
    assert weeks == [
        {"lineId": "line-commuter-rail", "count": 10, "timestamp": 1, "date": "2026-09-07"},
        {"lineId": "line-commuter-rail", "count": 25, "timestamp": 1, "date": "2026-09-14"},
    ]


def test_no_commuter_rail_data():
    assert landing.combine_commuter_rail_weeks([[], []]) == []
