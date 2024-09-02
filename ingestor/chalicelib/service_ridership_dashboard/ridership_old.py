from typing import List
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from math import isnan


@dataclass
class RidershipEntry:
    date: date
    ridership: float


def create_ridership_entry_from_dict(entry_dict):
    return RidershipEntry(
        date=datetime.strptime(entry_dict["date"], "%Y-%m-%d").date(),
        ridership=float(entry_dict["riders"]),
    )


def get_valid_ridership_value(entries: List[RidershipEntry], start_lookback_idx: int):
    idx = start_lookback_idx
    while idx >= 0:
        entry = entries[idx]
        if not isnan(entry.ridership):
            return int(entry.ridership)
        idx -= 1


def create_time_series_from_entries(
    entries: List[RidershipEntry],
    start_date: date,
    end_date: date,
):
    series = []
    for idx, entry in enumerate(entries):
        if entry.date < start_date:
            continue
        ridership = get_valid_ridership_value(entries, idx)
        entry_start_date = max(start_date, entry.date)
        entry_end_date = min(
            end_date if idx == len(entries) - 1 else entries[idx + 1].date - timedelta(days=1),
            end_date,
        )
        today = entry_start_date
        while today <= entry_end_date:
            series.append(ridership)
            today += timedelta(days=1)
    return series


def get_ridership_time_series_by_adhoc_label(
    source: "RidershipSource",
    start_date: date,
    end_date: date,
):
    res = {}
    ridership_dict = get_ridership_json(source)
    for label, entries_json in ridership_dict.items():
        entries = [create_ridership_entry_from_dict(e) for e in entries_json]
        res[label] = create_time_series_from_entries(entries, start_date, end_date)
    return res


_adhoc_labels_map = {
    "Green-B": "Green Line",
    "Green-C": "Green Line",
    "Green-D": "Green Line",
    "Green-E": "Green Line",
    "Blue": "Blue Line",
    "Orange": "Orange Line",
    "Red": "Red Line",
}


def map_route_id_to_adhoc_label(route_id: str):
    return _adhoc_labels_map.get(route_id) or route_id
