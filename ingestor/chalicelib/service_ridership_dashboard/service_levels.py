from dataclasses import dataclass
from datetime import date
from tqdm import tqdm

from .queries import query_scheduled_service, ScheduledServiceRow
from .gtfs import RoutesByLine
from .util import bucket_by, index_by, date_range, date_to_string


@dataclass
class ServiceLevelsEntry:
    line_id: str
    line_short_name: str
    line_long_name: str
    route_ids: list[str]
    service_levels: list[int]
    has_service_exceptions: bool
    date: date


ServiceLevelsByDate = dict[date, ServiceLevelsEntry]
ServiceLevelsByLineId = dict[str, ServiceLevelsByDate]


def _divide_by_two_to_get_unidirectional_trip_counts(trip_counts: list[int]):
    return [count / 2 for count in trip_counts]


def _get_trip_count_by_hour_totals_for_day(rows_for_day: list[ScheduledServiceRow]) -> list[int]:
    by_hour_counts_for_day: list[list[int]] = [item["byHour"]["totals"] for item in rows_for_day]
    bidirectional_trip_counts = [sum(hour_values) for hour_values in zip(*by_hour_counts_for_day)]
    return _divide_by_two_to_get_unidirectional_trip_counts(bidirectional_trip_counts)


def _get_has_service_exception(rows_for_day: list[ScheduledServiceRow]) -> bool:
    return any(item.get("hasServiceExceptions") for item in rows_for_day)


def get_service_level_entries_by_line_id(
    routes_by_line: RoutesByLine,
    start_date: date,
    end_date: date,
) -> ServiceLevelsByLineId:
    entries: dict[str, list[ServiceLevelsEntry]] = {}
    for line, routes in (progress := tqdm(routes_by_line.items())):
        entries.setdefault(line.line_id, [])
        progress.set_description(f"Loading service levels for {line.line_id}")
        results_by_date_str: dict[str, list[ScheduledServiceRow]] = bucket_by(
            [
                row
                for route in routes
                for row in query_scheduled_service(
                    start_date=start_date,
                    end_date=end_date,
                    route_id=route.route_id,
                )
            ],
            lambda row: row["date"],
        )
        for today in date_range(start_date, end_date):
            today_str = date_to_string(today)
            all_service_levels_today = results_by_date_str.get(today_str, [])
            entry = ServiceLevelsEntry(
                date=today,
                line_id=line.line_id,
                line_short_name=line.line_short_name,
                line_long_name=line.line_long_name,
                route_ids=[route.route_id for route in routes],
                service_levels=_get_trip_count_by_hour_totals_for_day(all_service_levels_today),
                has_service_exceptions=_get_has_service_exception(all_service_levels_today),
            )
            entries[line.line_id].append(entry)
    return {line_id: index_by(entries, lambda e: e.date) for line_id, entries in entries.items()}
