from datetime import date

from .util import date_to_string
from .types import LineData, SummaryData
from .config import PRE_COVID_DATE


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


def _merge_time_series(many_series: list[list[float]]) -> list[float]:
    min_length = min(len(series) for series in many_series)
    entries = [0.0] * min_length
    for series in many_series:
        for idx, value in enumerate(series):
            if idx >= min_length:
                break
            entries[idx] += value
    return [round(e) for e in entries]


def _get_fraction_of_timeseries_value(
    time_series: list[float],
    start_date: date,
    present_date: date,
    denominator_date: date,
) -> float:
    present_idx = min(
        (present_date - start_date).days,
        len(time_series) - 1,
    )
    denominator_idx = (denominator_date - start_date).days
    numerator = time_series[present_idx]
    denominator = time_series[denominator_idx]
    return numerator / denominator if denominator != 0 else 0.0


def get_summary_data(line_data: list[LineData], start_date: date, end_date: date) -> SummaryData:
    total_ridership_history = _merge_time_series([line["ridershipHistory"] for line in line_data])
    total_service_history = _merge_time_series([line["serviceHistory"] for line in line_data])
    total_passengers = total_ridership_history[-1]
    total_trips = total_service_history[-1]
    total_routes_cancelled = sum(_line_is_cancelled(line) for line in line_data)
    total_reduced_service = sum(_line_has_reduced_service(line) for line in line_data)
    total_increased_service = sum(_line_has_increased_service(line) for line in line_data)
    return {
        "totalRidershipHistory": total_ridership_history,
        "totalServiceHistory": total_service_history,
        "totalRidershipPercentage": _get_fraction_of_timeseries_value(
            time_series=total_ridership_history,
            start_date=start_date,
            present_date=end_date,
            denominator_date=PRE_COVID_DATE,
        ),
        "totalServicePercentage": _get_fraction_of_timeseries_value(
            time_series=total_service_history,
            start_date=start_date,
            present_date=end_date,
            denominator_date=PRE_COVID_DATE,
        ),
        "totalPassengers": total_passengers,
        "totalTrips": total_trips,
        "totalRoutesCancelled": total_routes_cancelled,
        "totalReducedService": total_reduced_service,
        "totalIncreasedService": total_increased_service,
        "startDate": date_to_string(start_date),
        "endDate": date_to_string(end_date),
    }
