import numpy as np
import pandas as pd

from ..agg_speed_tables import fleet_cols, group_data_by_date_and_branch, group_weekly_data


def _row(route, date, miles, **fleet):
    return {
        "route": route,
        "date": date,
        "line": "line-green",
        "miles_covered": miles,
        "total_time": 1.0,
        "count": 1.0,
        **fleet,
    }


def test_fleet_cols():
    assert fleet_cols(["count", "avg_car_age", "pct_new_trips", "fleet_mix_type7", "line"]) == [
        "avg_car_age",
        "pct_new_trips",
        "fleet_mix_type7",
    ]


def test_weekly_fleet_mix_mean_and_missing_days():
    mix_a = {"fleet_mix_type7": 60.0, "fleet_mix_type9": 40.0}
    mix_b = {"fleet_mix_type7": 40.0, "fleet_mix_type9": 60.0}
    rows = [
        # Week of Mon 2026-09-07: two days with mix data (copied onto each branch row)
        _row("line-green-b", "2026-09-07", 1.0, **mix_a),
        _row("line-green-c", "2026-09-07", 1.0, **mix_a),
        _row("line-green-b", "2026-09-08", 1.0, **mix_b),
        _row("line-green-c", "2026-09-08", 1.0, **mix_b),
        # Week of Mon 2026-09-14: no fleet data at all
        _row("line-green-b", "2026-09-14", 1.0),
        _row("line-green-c", "2026-09-14", 1.0),
    ]
    df = group_data_by_date_and_branch(pd.DataFrame(rows))
    weeks = group_weekly_data(df, "2026-09-07")

    assert weeks[0]["fleet_mix_type7"] == 50.0
    assert weeks[0]["fleet_mix_type9"] == 50.0
    assert np.isclose(weeks[0]["fleet_mix_type7"] + weeks[0]["fleet_mix_type9"], 100)
    # No data means the attribute is omitted, not zero-filled
    assert "fleet_mix_type7" not in weeks[1]
    assert weeks[1]["miles_covered"] == 2.0
