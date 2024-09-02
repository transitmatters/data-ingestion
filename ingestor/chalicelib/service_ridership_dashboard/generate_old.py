import json
from datetime import date, datetime, timedelta
from typing import TypedDict
from dataclasses import dataclass
from tqdm import tqdm
from math import isnan

from .config import (
    OUTPUT_FILE,
    START_DATE,
    PRE_COVID_DATE,
    IGNORE_LINE_IDS,
    FILL_DATE_RANGES,
    ANOMALY_RANGES,
    TIME_ZONE,
)
from .queries import query_scheduled_service, query_ridership
from .gtfs import get_routes_by_line
from .util import (
    bucket_by,
    date_from_string,
    date_range_contains,
    date_range,
    date_to_string,
    get_date_ranges_of_same_value,
    index_by,
)
from .service_levels import ServiceLevelsEntry, get_service_level_entries_by_line_id


class ServiceRegimeEntry(TypedDict):
    cancelled: bool
    tripsPerHour: list[int]
    totalTrips: int


class ServiceRegime(TypedDict):
    weekday: ServiceRegimeEntry
    saturday: ServiceRegimeEntry
    sunday: ServiceRegimeEntry


class LineData(TypedDict):
    id: str
    shortName: str
    longName: str
    routeIds: list[str]
    startDate: str
    lineKind: str
    ridershipHistory: list[int]
    serviceHistory: list[int]
    serviceRegimes: dict[str, ServiceRegime]


@dataclass
class RidershipEntry:
    date: date
    ridership: float


def get_line_kind(route_ids: list[str], line_id: str):
    if line_id.startswith("line-Boat"):
        return "boat"
    if any((r for r in route_ids if r.lower().startswith("cr-"))):
        return "regional-rail"
    if line_id.startswith("line-SL"):
        return "silver"
    if line_id in ("line-Red", "line-Orange", "line-Blue", "line-Green"):
        return line_id.split("-")[1].lower()
    return "bus"


def get_ridership_entries_for_line_id(start_date: date, end_date: date, line_id: str) -> list[RidershipEntry]:
    entries = query_ridership(
        start_date=start_date,
        end_date=end_date,
        line_id=line_id,
    )
    return [
        RidershipEntry(
            date=date_from_string(entry["date"]),
            ridership=entry["count"],
        )
        for entry in entries
    ]


def get_valid_ridership_value(entries: list[RidershipEntry], start_lookback_idx: int):
    idx = start_lookback_idx
    while idx >= 0:
        entry = entries[idx]
        if not isnan(entry.ridership):
            return int(entry.ridership)
        idx -= 1


def get_ridership_time_series_for_line_id(start_date: date, end_date: date, line_id: str) -> list[int]:
    time_series: list[int] = []
    entries = get_ridership_entries_for_line_id(
        start_date=start_date,
        end_date=end_date,
        line_id=line_id,
    )
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
            time_series.append(ridership)
            today += timedelta(days=1)
    return time_series


def get_ridership_time_series_by_line_id(start_date: date, end_date: date, line_ids: list[str]):
    ridership_time_series_by_line_id: dict[str, list[int]] = {}
    for line_id in (progress := tqdm(line_ids)):
        progress.set_description(f"Loading ridership for {line_id}")
        entries_for_line_id = get_ridership_time_series_for_line_id(
            start_date=start_date,
            end_date=end_date,
            line_id=line_id,
        )
        ridership_time_series_by_line_id[line_id] = entries_for_line_id
    return ridership_time_series_by_line_id


# def get_service_level_entries_and_line_ids(feeds_and_service_levels: list[Tuple[GtfsFeed, Dict]]):
#     entries = []

#     for feed, service_levels in feeds_and_service_levels:
#         for line_id, line_entry in service_levels.items():
#             all_line_ids.add(line_id)
#             service_level_history = line_entry["history"]
#             line_long_name = line_entry["longName"]
#             line_short_name = line_entry["shortName"]
#             route_ids = line_entry["routeIds"]
#             exception_dates = list(map(date_from_string, line_entry["exceptionDates"]))
#             for service_levels in service_level_history:
#                 entry = ServiceLevelsEntry(
#                     line_id=line_id,
#                     line_short_name=line_short_name,
#                     line_long_name=line_long_name,
#                     route_ids=route_ids,
#                     service_levels=service_levels["serviceLevels"],
#                     start_date=date_from_string(service_levels["startDate"]),
#                     end_date=date_from_string(service_levels["endDate"]),
#                     exception_dates=exception_dates,
#                     feed=feed,
#                 )
#                 entries.append(entry)
#     return bucket_by(entries, lambda e: e.line_id), all_line_ids


def get_service_levels_entry_for_date(entries: list[ServiceLevelsEntry], date: date):
    matching_entries = [e for e in entries if e.date <= date <= e.end_date]
    if len(matching_entries) > 0:
        return max(matching_entries, key=lambda e: e.feed.start_date)
    return None


def get_service_level_history(
    entries: list[ServiceLevelsEntry],
    start_date: date,
    end_date: date,
):
    levels_by_date = {}
    date = start_date
    while date <= end_date:
        entry = get_service_levels_entry_for_date(entries, date)
        levels_by_date[date] = round(sum(entry.service_levels)) if entry else 0
        date += timedelta(days=1)
    values = []
    for date_range, value in get_date_ranges_of_same_value(levels_by_date):
        (min_date, max_date) = date_range
        range_length_days = 1 + (max_date - min_date).days
        is_weekend = range_length_days <= 2 and all((d.weekday() in (5, 6) for d in (min_date, max_date)))
        is_fill_range = any((date_range_contains(fill_range, date_range) for fill_range in FILL_DATE_RANGES))
        is_ignore_range = any((date_range_contains(ignore_range, date_range) for ignore_range in ANOMALY_RANGES))
        fill_hole = value == 0 and (range_length_days <= 5 or is_fill_range)
        value_to_append = values[-1] if len(values) and (fill_hole or is_ignore_range or is_weekend) else value
        values += range_length_days * [value_to_append]
    return values


def get_exemplar_service_levels_for_lookback_date(
    entries: list[ServiceLevelsEntry],
    start_lookback_date: date,
    matching_days_of_week: list[int],
):
    date = start_lookback_date
    while date >= PRE_COVID_DATE:
        entry = get_service_levels_entry_for_date(entries, date)
        if (
            entry
            and sum(entry.service_levels) > 0
            and not date in entry.exception_dates
            and date.weekday() in matching_days_of_week
        ):
            return entry.service_levels
        date -= timedelta(days=1)
    return None


def service_is_cancelled(
    entries: list[ServiceLevelsEntry],
    start_lookback_date: date,
    matching_days_of_week: list[int],
):
    most_recent_matching_date = start_lookback_date
    while most_recent_matching_date.weekday() not in matching_days_of_week:
        most_recent_matching_date -= timedelta(days=1)
    entry = get_service_levels_entry_for_date(entries, most_recent_matching_date)
    return entry is None


def get_service_levels_summary_dict(
    entries: list[ServiceLevelsEntry],
    start_lookback_date: date,
    matching_days_of_week: list[int],
):
    if service_is_cancelled(entries, start_lookback_date, matching_days_of_week):
        return {"cancelled": True, "tripsPerHour": None, "totalTrips": 0}
    trips_per_hour = get_exemplar_service_levels_for_lookback_date(
        entries,
        start_lookback_date,
        matching_days_of_week,
    )
    total_trips = round(sum(trips_per_hour)) if trips_per_hour else 0
    return {
        "cancelled": False,
        "tripsPerHour": trips_per_hour,
        "totalTrips": total_trips,
    }


def get_service_regime_dict(entries: list[ServiceLevelsEntry], start_lookback_date: date):
    return {
        "weekday": get_service_levels_summary_dict(entries, start_lookback_date, list(range(0, 5))),
        "saturday": get_service_levels_summary_dict(entries, start_lookback_date, [5]),
        "sunday": get_service_levels_summary_dict(entries, start_lookback_date, [6]),
    }


def count_total_trips(regime_dict):
    return (
        regime_dict["weekday"]["totalTrips"]
        + regime_dict["saturday"]["totalTrips"]
        + regime_dict["sunday"]["totalTrips"]
    )


def summarize_service(numerator_regime_dict, denominator_regime_dict):
    numerator_total_trips = count_total_trips(numerator_regime_dict)
    denominator_total_trips = count_total_trips(denominator_regime_dict)
    try:
        total_trips_fraction = numerator_total_trips / denominator_total_trips
    except ZeroDivisionError:
        total_trips_fraction = 0
    return numerator_total_trips, total_trips_fraction


def get_merged_ridership_time_series(
    route_ids: list[str],
    ridership_time_series_by_label: dict[str, list[int]],
):
    labels = set((map_route_id_to_adhoc_label(route_id) for route_id in route_ids))
    matching_time_series = [
        ridership_time_series_by_label.get(label) for label in labels if label in ridership_time_series_by_label
    ]
    if len(matching_time_series) == 0:
        return None
    merged_time_series = [0] * len(matching_time_series[0])
    for ts in matching_time_series:
        for idx, value in enumerate(ts):
            merged_time_series[idx] += value
    return merged_time_series


def get_ridership_percentage(total_ridership_time_series):
    ridership_percentage = round(total_ridership_time_series[-1] / total_ridership_time_series[0], 2)
    return ridership_percentage


def get_service_percentage(total_service_time_series):
    service_percentage = round(total_service_time_series[-1] / total_service_time_series[0], 2)
    return service_percentage


# since the data holds constant over a week, this function removes the duplicates of 7 and condenses into 1 datapoint
# keeps overall trends the same
def condensed_time_series(total_time_series):
    condensed_series = [total_time_series[0]]
    for i in range(len(total_time_series) - 1):
        if total_time_series[i] is not total_time_series[i + 1]:
            condensed_series.append(total_time_series[i + 1])
    return condensed_series


def generate_total_data(
    ridership_time_series_list: list[list[float]],
    service_time_series_list: list[list[float]],
    combined_total_trips: int,
    total_cancelled_routes: int,
    total_reduced_serv_routes: int,
    total_increased_serv_routes: int,
    start_date: date,
):
    total_ridership_time_series = [sum(entries_for_day) for entries_for_day in zip(*ridership_time_series_list)]
    condensed_ridership_series = condensed_time_series(total_ridership_time_series)
    total_service_time_series = [sum(entries_for_day) for entries_for_day in zip(*service_time_series_list)]
    condensed_service_series = condensed_time_series(total_service_time_series)
    total_ridership_percentage = get_ridership_percentage(total_ridership_time_series)
    total_service_percentage = get_service_percentage(total_service_time_series)
    total_passengers = total_ridership_time_series[-1]
    end_date = start_date + timedelta(days=(len(total_service_time_series) - 1))
    return {
        "totalRidershipHistory": condensed_ridership_series,
        "totalServiceHistory": condensed_service_series,
        "totalRidershipPercentage": total_ridership_percentage,
        "totalServicePercentage": total_service_percentage,
        "totalPassengers": total_passengers,
        "totalTrips": combined_total_trips,
        "totalRoutesCancelled": total_cancelled_routes,
        "totalReducedService": total_reduced_serv_routes,
        "totalIncreasedService": total_increased_serv_routes,
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
    }


def generate_data_file(start_date: date, end_date: date):
    data_by_line_id = {}
    entries, line_ids = get_service_level_entries_and_line_ids(feeds_and_service_levels)
    # feeds_and_service_levels = load_feeds_and_service_levels_from_archive()
    ridership_time_series_by_label = get_ridership_time_series_by_adhoc_label(
        ridership_source,
        start_date,
        end_date,
    )
    ridership_time_series_list = []
    service_time_series_list = []
    combined_total_trips = 0
    total_reduced_serv_routes = 0
    total_increased_serv_routes = 0
    total_cancelled_routes = 0
    for line_id in line_ids:
        if line_id in IGNORE_LINE_IDS:
            continue
        entries_for_line_id = entries[line_id]
        exemplar_entry = entries_for_line_id[-1]
        ridership_time_series = get_merged_ridership_time_series(
            exemplar_entry.route_ids,
            ridership_time_series_by_label,
        )
        service_time_series = get_service_level_history(
            entries_for_line_id,
            start_date,
            end_date,
        )
        baseline_service_regime = get_service_regime_dict(
            entries_for_line_id,
            start_date,
        )
        current_service_regime = get_service_regime_dict(entries_for_line_id, end_date)
        day_kinds = ("weekday", "saturday", "sunday")

        try:
            service_time_fraction = sum((current_service_regime[day]["totalTrips"] for day in day_kinds)) / sum(
                (baseline_service_regime[day]["totalTrips"] for day in day_kinds)
            )
        except ZeroDivisionError:
            service_time_fraction = 0
        if service_time_fraction > 1:
            total_increased_serv_routes += 1
        elif service_time_fraction < 1 and service_time_fraction != 0:
            total_reduced_serv_routes += 1

        if (
            current_service_regime["weekday"]["cancelled"]
            or current_service_regime["saturday"]["cancelled"]
            or current_service_regime["sunday"]["cancelled"]
        ):
            total_cancelled_routes += 1

        total_trips, service_fraction = summarize_service(
            current_service_regime,
            baseline_service_regime,
        )

        if ridership_time_series is not None and service_time_series is not None:
            ridership_time_series_list.append(ridership_time_series)
            service_time_series_list.append(service_time_series)
        combined_total_trips += total_trips
        data_by_line_id[line_id] = {
            "id": line_id,
            "shortName": exemplar_entry.line_short_name,
            "longName": exemplar_entry.line_long_name,
            "routeIds": exemplar_entry.route_ids,
            "startDate": start_date.strftime("%Y-%m-%d"),
            "lineKind": get_line_kind(exemplar_entry.route_ids, line_id),
            "ridershipHistory": ridership_time_series,
            "serviceHistory": service_time_series,
            "serviceFraction": service_fraction,
            "totalTrips": total_trips,
            "serviceRegimes": {
                "baseline": baseline_service_regime,
                "current": current_service_regime,
            },
        }

    total_data = generate_total_data(
        ridership_time_series_list,
        service_time_series_list,
        combined_total_trips,
        total_cancelled_routes,
        total_reduced_serv_routes,
        total_increased_serv_routes,
        start_date,
    )

    with open(OUTPUT_FILE, "w") as file:
        file.write(
            json.dumps(
                {
                    "summaryData": total_data,
                    "lineData": data_by_line_id,
                }
            )
        )


def create_service_ridership_dash_json(
    start_date: date = START_DATE,
    end_date: date = datetime.now(TIME_ZONE).date(),
):
    # service_level_entries = get_service_level_entries_by_line_id(
    #     start_date=start_date,
    #     end_date=end_date,
    # )
    # line_ids = list(service_level_entries.keys())
    line_ids = ["line-Red", "line-Orange", "line-Blue", "line-Green"]
    ridership_time_series_by_line_id = get_ridership_time_series_by_line_id(
        start_date=start_date,
        end_date=end_date,
        line_ids=line_ids,
    )
    print(ridership_time_series_by_line_id)


if __name__ == "__main__":
    create_service_ridership_dash_json()
