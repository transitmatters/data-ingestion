from datetime import date
from decimal import Decimal

from ..reliability.excess_trip_time import FIELDS, aggregate_rows, to_items
from ..reliability.the_ride import read_csv_rows

HEADER = (
    "svc_date,day,halfhour,peak,route_id,dir_id,est_pax,weighted_bl_trip_time,weighted_bl_wait_time,"
    "weighted_bl_travel_time,weighted_trip_time,weighted_wait_time,weighted_travel_time,five_min_thresh,"
    "twenty_min_thresh,twenty_pct_thresh,thirty_pct_thresh\n"
)

CSV = HEADER + "\n".join(
    [
        "2026-09-29,Weekday,07:30:00,AM PEAK,Red,NB,100,1500,500,1000,1700,600,1100,80,100,70,90",
        "2026-09-29,Weekday,07:30:00,AM PEAK,Red,SB,50,700,200,500,760,240,520,45,50,40,48",
        "2026-09-29,Weekday,12:00:00,OFF-PEAK,Red,NB,20,300,100,200,310,90,220,20,20,19,20",
        "2026-09-29,Weekday,12:00:00,OFF-PEAK,Blue,WB,10.25,120,40,80,150,50,100,9.5,10.25,8,9",
        "2026-09-29,Weekday,12:00:00,OFF-PEAK,Green,WB,30,300,100,200,330,110,220,28,30,25,27",
        "2026-09-01,Weekday,12:00:00,OFF-PEAK,Red,NB,99,1,1,1,1,1,1,1,1,1,1",
    ]
)


def _items(lookback_days=None):
    return to_items(aggregate_rows(read_csv_rows(CSV), lookback_days, today=date(2026, 10, 1)))


def test_sums_half_hours_into_line_days():
    items = {(item["routeId"], item["date"]): item for item in _items()}
    assert set(items) == {("line-red", "2026-09-29"), ("line-red", "2026-09-01"), ("line-blue", "2026-09-29")}
    red = items[("line-red", "2026-09-29")]
    assert red["mode"] == "subway"
    assert red["riders"] == Decimal("170.0")
    assert red["waitTime"] == Decimal("930.0")
    assert red["travelTime"] == Decimal("1840.0")
    assert red["baselineTripTime"] == Decimal("2500.0")
    assert red["withinFiveMin"] == Decimal("145.0")
    assert red["withinThirtyPct"] == Decimal("158.0")


def test_splits_by_gtfs_direction_and_period():
    red = next(item for item in _items() if item["routeId"] == "line-red" and item["date"] == "2026-09-29")
    # NB is GTFS direction 1 and SB is 0, matching the HeadwayReliability table
    assert red["byDirection"]["1"]["riders"] == Decimal("120.0")
    assert red["byDirection"]["0"]["riders"] == Decimal("50.0")
    assert red["byPeriod"]["amPeak"]["riders"] == Decimal("150.0")
    assert red["byPeriod"]["offPeak"]["riders"] == Decimal("20.0")
    assert red["byPeriod"]["amPeak"]["withinFiveMin"] == Decimal("125.0")
    # Each direction has its own per-period sums, so a direction filter applies to them too
    assert red["byDirection"]["1"]["byPeriod"]["amPeak"]["riders"] == Decimal("100.0")
    assert red["byDirection"]["1"]["byPeriod"]["offPeak"]["riders"] == Decimal("20.0")
    assert red["byDirection"]["0"]["byPeriod"]["offPeak"]["riders"] == Decimal("0.0")


def test_every_item_has_every_direction_and_period():
    """So summing a span on the dashboard never drops a direction or period that one day lacks."""
    blue = next(item for item in _items() if item["routeId"] == "line-blue")
    assert set(blue["byDirection"]) == {"0", "1"}
    assert set(blue["byPeriod"]) == {"amPeak", "pmPeak", "offPeak"}
    zeros = {field: Decimal("0.0") for field in FIELDS.values()}
    assert blue["byDirection"]["1"] == {**zeros, "byPeriod": {"amPeak": zeros, "pmPeak": zeros, "offPeak": zeros}}
    assert set(blue["byDirection"]["0"]["byPeriod"]) == {"amPeak", "pmPeak", "offPeak"}
    assert blue["byPeriod"]["pmPeak"]["riders"] == Decimal("0.0")
    assert blue["riders"] == Decimal("10.2")


def test_skips_unknown_lines(capsys):
    assert all(item["routeId"] != "line-green" for item in _items())
    assert "'Green'" in capsys.readouterr().out


def test_lookback_drops_older_days():
    assert {item["date"] for item in _items(lookback_days=7)} == {"2026-09-29"}
