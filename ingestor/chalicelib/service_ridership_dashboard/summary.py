from datetime import date

from .time_series import (
    get_latest_weekly_median_time_series_entry,
    merge_weekly_median_time_series,
)
from .types import LineData, SummaryData
from .util import date_to_string


def _line_is_cancelled(line: LineData) -> bool:
    return line["serviceRegimes"]["current"]["weekday"]["cancelled"]


def _line_has_reduced_service(line: LineData) -> bool:
    try:
        weekday_service_last_year = line["serviceRegimes"]["oneYearAgo"]["weekday"]["totalTrips"]
        weekday_service_present = line["serviceRegimes"]["current"]["weekday"]["totalTrips"]
        return weekday_service_present / weekday_service_last_year < (19 / 20)
    except ZeroDivisionError:
        return False


def _line_has_increased_service(line: LineData) -> bool:
    try:
        weekday_service_last_year = line["serviceRegimes"]["oneYearAgo"]["weekday"]["totalTrips"]
        weekday_service_present = line["serviceRegimes"]["current"]["weekday"]["totalTrips"]
        return weekday_service_present / weekday_service_last_year > (20 / 19)
    except ZeroDivisionError:
        return False


def get_summary_data(line_data: list[LineData], start_date: date, end_date: date) -> SummaryData:
    total_ridership_history = merge_weekly_median_time_series([line["ridershipHistory"] for line in line_data])
    total_service_history = merge_weekly_median_time_series([line["serviceHistory"] for line in line_data])
    total_passengers = get_latest_weekly_median_time_series_entry(total_ridership_history)
    total_trips = get_latest_weekly_median_time_series_entry(total_service_history)
    total_routes_cancelled = sum(_line_is_cancelled(line) for line in line_data)
    total_reduced_service = sum(_line_has_reduced_service(line) for line in line_data)
    total_increased_service = sum(_line_has_increased_service(line) for line in line_data)
    return {
        "totalRidershipHistory": total_ridership_history,
        "totalServiceHistory": total_service_history,
        "totalRidershipPercentage": 0,  # From CRD, remove
        "totalServicePercentage": 0,  # From CRD, remove
        "totalPassengers": total_passengers or 0,
        "totalTrips": total_trips or 0,
        "totalRoutesCancelled": total_routes_cancelled,
        "totalReducedService": total_reduced_service,
        "totalIncreasedService": total_increased_service,
        "startDate": date_to_string(start_date),
        "endDate": date_to_string(end_date),
    }
