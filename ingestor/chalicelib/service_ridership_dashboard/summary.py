from datetime import date, timedelta
from typing import Optional

from .time_series import (
    get_latest_weekly_median_time_series_entry,
    merge_weekly_median_time_series,
)
from .types import LineData, ModeKind, SummaryData, WeeklyMedianTimeSeries
from .util import bucket_by, date_to_string, line_kind_to_mode_kind

# Lines whose ridership ends more than this before the newest line's are treated as stopped
# (discontinued, or a source that has stopped updating) rather than one week behind.
RIDERSHIP_LAG_WEEKS = 4


def _latest_week_all_lines_report(histories: list[WeeklyMedianTimeSeries]) -> Optional[str]:
    """The latest week every recently reporting line has ridership for.

    Sources publish on different schedules, so the newest week often has subway but not yet
    bus, CR or ferry. Summing that week alone undercounts the headline total.
    """
    latest_by_line = [max(history) for history in histories if history]
    if not latest_by_line:
        return None
    newest = date.fromisoformat(max(latest_by_line))
    cutoff = date_to_string(newest - timedelta(weeks=RIDERSHIP_LAG_WEEKS))
    return min(week for week in latest_by_line if week >= cutoff)


def _line_is_cancelled(line: LineData) -> bool:
    """Check whether a line's current weekday service is cancelled.

    Args:
        line: A LineData dictionary containing service regime information.

    Returns:
        True if the line's current weekday service is cancelled.
    """
    return line["serviceRegimes"]["current"]["weekday"]["cancelled"]


def _line_has_reduced_service(line: LineData) -> bool:
    """Check whether a line has reduced weekday service compared to one year ago.

    A line is considered to have reduced service if current weekday trips are
    less than 95% (19/20) of the trips from one year ago.

    Args:
        line: A LineData dictionary containing service regime information.

    Returns:
        True if the line has reduced service, False otherwise.
    """
    try:
        weekday_service_last_year = line["serviceRegimes"]["oneYearAgo"]["weekday"]["totalTrips"]
        weekday_service_present = line["serviceRegimes"]["current"]["weekday"]["totalTrips"]
        return weekday_service_present / weekday_service_last_year < (19 / 20)
    except ZeroDivisionError:
        return False


def _line_has_increased_service(line: LineData) -> bool:
    """Check whether a line has increased weekday service compared to one year ago.

    A line is considered to have increased service if current weekday trips are
    more than approximately 105% (20/19) of the trips from one year ago.

    Args:
        line: A LineData dictionary containing service regime information.

    Returns:
        True if the line has increased service, False otherwise.
    """
    try:
        weekday_service_last_year = line["serviceRegimes"]["oneYearAgo"]["weekday"]["totalTrips"]
        weekday_service_present = line["serviceRegimes"]["current"]["weekday"]["totalTrips"]
        return weekday_service_present / weekday_service_last_year > (20 / 19)
    except ZeroDivisionError:
        return False


def get_summary_data(line_data: list[LineData], start_date: date, end_date: date) -> SummaryData:
    """Compute aggregate summary statistics across all lines for the dashboard.

    Args:
        line_data: A list of LineData dictionaries for all lines.
        start_date: The start date of the data range.
        end_date: The end date of the data range.

    Returns:
        A SummaryData dict with totals for ridership, service, and route status counts.
    """
    ridership_histories = [line["ridershipHistory"] or {} for line in line_data]
    total_ridership_history = merge_weekly_median_time_series(ridership_histories)
    total_service_history = merge_weekly_median_time_series([line["serviceHistory"] for line in line_data])
    passengers_week = _latest_week_all_lines_report(ridership_histories)
    total_passengers = total_ridership_history.get(passengers_week) if passengers_week else None
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


def get_summary_data_by_mode(
    line_data: list[LineData],
    start_date: date,
    end_date: date,
) -> dict[ModeKind, SummaryData]:
    lines_by_mode = bucket_by(line_data, lambda line: line_kind_to_mode_kind(line["lineKind"]))
    mode_summary_data: dict[ModeKind, SummaryData] = {}
    for mode, lines in lines_by_mode.items():
        mode_summary_data[mode] = get_summary_data(lines, start_date, end_date)
    return mode_summary_data
