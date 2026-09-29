from datetime import date
from decimal import Decimal

from ..reliability.commuter_rail import aggregate_rows, to_items
from ..reliability.the_ride import (
    filter_lookback,
    merge_entries,
    parse_current_row,
    parse_legacy_row,
    read_csv_rows,
)

RIDE_CSV = """﻿TripDate,TotalRequests,CompletedTrips,CompletedOnTime,TotalNoShows,TotalMissedTrips,OTP,ObjectId
2025/12/01 05:00:00+00,4800,3558,3388,0,0,95.22,1
2026/08/31 04:00:00+00,4986,3701,3606,110,22,96.95,2
"""

RIDE_LEGACY_CSV = """trip_date,ontime_trip_count,trip_count,ObjectId
2014/07/01 04:00:00+00,5479,5884,1
2025/12/01 05:00:00+00,1,2,2
"""

CR_HEADER = "service_date,gtfs_route_id,gtfs_route_long_name,peak_offpeak_ind,metric_type,otp_numerator,otp_denominator,cancelled_numerator\n"


def _cr_rows(lines):
    return read_csv_rows(CR_HEADER + "\n".join(lines))


def test_ride_parses_current_dataset():
    rows = list(read_csv_rows(RIDE_CSV))
    entry = parse_current_row(rows[1])
    assert entry.to_item() == {
        "lineId": "line-RIDE",
        "date": "2026-08-31",
        "timestamp": entry.to_item()["timestamp"],
        "completed": 3701,
        "onTime": 3606,
        "source": "current",
        "requests": 4986,
        "noShows": 110,
        "missed": 22,
        "otp": Decimal("96.95"),
    }


def test_ride_omits_unrecorded_no_shows_and_missed_trips():
    item = parse_current_row(list(read_csv_rows(RIDE_CSV))[0]).to_item()
    assert item["date"] == "2025-12-01"
    assert "noShows" not in item and "missed" not in item
    assert item["requests"] == 4800


def test_ride_current_dataset_wins_over_legacy():
    current = [parse_current_row(row) for row in read_csv_rows(RIDE_CSV)]
    legacy = [parse_legacy_row(row) for row in read_csv_rows(RIDE_LEGACY_CSV)]
    merged = merge_entries(current, legacy)
    assert [(e.date.isoformat(), e.source, e.completed) for e in merged] == [
        ("2014-07-01", "legacy", 5884),
        ("2025-12-01", "current", 3558),
        ("2026-08-31", "current", 3701),
    ]
    assert "otp" not in merged[0].to_item()


def test_ride_lookback_filter():
    entries = [parse_current_row(row) for row in read_csv_rows(RIDE_CSV)]
    assert [e.date for e in filter_lookback(entries, 30, today=date(2026, 9, 28))] == [date(2026, 8, 31)]
    assert len(filter_lookback(entries, None)) == 2


def test_cr_combines_periods_and_branches():
    rows = _cr_rows(
        [
            "6/30/2026,CR-NewBedford,Fall River Line,PEAK,Headway / Schedule Adherence,5,6,1",
            "6/30/2026,CR-NewBedford,New Bedford Line,PEAK,Headway / Schedule Adherence,4,4,0",
            "6/30/2026,CR-NewBedford,New Bedford Line,OFF_PEAK,Headway / Schedule Adherence,10,12,2",
            "6/30/2026,CR-NewBedford,New Bedford Line,OFF_PEAK,Some Future Metric,99,99,99",
        ]
    )
    [item] = to_items(aggregate_rows(rows))
    assert item["routeId"] == "CR-NewBedford"
    assert item["date"] == "2026-06-30"
    assert item["peak"] == {"otpNumerator": 9, "otpDenominator": 10, "cancelled": 1}
    assert item["offPeak"] == {"otpNumerator": 10, "otpDenominator": 12, "cancelled": 2}
    assert (item["otpNumerator"], item["otpDenominator"], item["cancelled"]) == (19, 22, 3)


def test_cr_lookback_and_passthrough_route_ids():
    rows = _cr_rows(
        [
            "1/1/2016,CR-Fairmount,Fairmount Line,OFF_PEAK,Headway / Schedule Adherence,33,34,0",
            "6/28/2026,CapeFlyer,CapeFLYER Line,OFF_PEAK,Headway / Schedule Adherence,2,2,0",
        ]
    )
    items = to_items(aggregate_rows(rows, lookback_days=180, today=date(2026, 9, 28)))
    assert [(i["routeId"], i["date"]) for i in items] == [("CapeFlyer", "2026-06-28")]
    assert items[0]["peak"] == {"otpNumerator": 0, "otpDenominator": 0, "cancelled": 0}
