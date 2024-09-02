import json
from datetime import date, datetime, timedelta
from typing import TypedDict, Literal

from .config import (
    START_DATE,
    TIME_ZONE,
    PRE_COVID_DATE,
)
from .service_levels import get_service_level_entries_by_line_id, ServiceLevelsByDate, ServiceLevelsEntry
from .ridership import ridership_by_line_id, RidershipByDate, RidershipEntry
from .service_summaries import ServiceSummary, summarize_weekly_service_around_date
from .util import date_to_string
from .time_series import get_time_series

LineKind = Literal["bus", "subway", "commuter-rail", "ferry"]


class ServiceRegimes(TypedDict):
    present: ServiceSummary
    oneYearAgo: ServiceSummary
    preCovid: ServiceSummary


class LineData(TypedDict):
    id: str
    shortName: str
    longName: str
    routeIds: list[str]
    startDate: str
    lineKind: LineKind
    ridershipHistory: list[float]
    serviceHistory: list[float]
    serviceRegimes: ServiceRegimes


class DashJSON(TypedDict):
    lineData: dict[str, LineData]


def create_service_regimes(
    service_levels: ServiceLevelsByDate,
    date: date,
) -> ServiceRegimes:
    return {
        "present": summarize_weekly_service_around_date(
            date=date,
            service_levels=service_levels,
        ),
        "oneYearAgo": summarize_weekly_service_around_date(
            date=date - timedelta(days=365),
            service_levels=service_levels,
        ),
        "preCovid": summarize_weekly_service_around_date(
            date=PRE_COVID_DATE,
            service_levels=service_levels,
        ),
    }


def create_line_data(
    start_date: date,
    end_date: date,
    service_levels: dict[date, ServiceLevelsEntry],
    ridership: dict[date, RidershipEntry],
) -> LineData:
    [latest_service_levels_date, *_] = sorted(service_levels.keys(), reverse=True)
    service_level_entry = service_levels[latest_service_levels_date]
    return {
        "id": service_level_entry.line_id,
        "shortName": service_level_entry.line_short_name,
        "longName": service_level_entry.line_long_name,
        "routeIds": service_level_entry.route_ids,
        "startDate": date_to_string(start_date),
        "lineKind": "bus",
        "ridershipHistory": get_time_series(
            entries=ridership,
            entry_value_getter=lambda entry: entry.ridership,
            start_date=start_date,
            max_end_date=end_date,
        ),
        "serviceHistory": get_time_series(
            entries=service_levels,
            entry_value_getter=lambda entry: sum(entry.service_levels),
            start_date=start_date,
            max_end_date=end_date,
        ),
        "serviceRegimes": create_service_regimes(
            service_levels=service_levels,
            date=latest_service_levels_date,
        ),
    }


def create_service_ridership_dash_json(
    start_date: date = START_DATE,
    end_date: date = datetime.now(TIME_ZONE).date(),
):
    service_level_entries = get_service_level_entries_by_line_id(
        start_date=start_date,
        end_date=end_date,
    )
    print("service_level_entries", service_level_entries)
    ridership_entries = ridership_by_line_id(
        start_date=start_date,
        end_date=end_date,
        line_ids=list(service_level_entries.keys()),
    )
    dash_json: DashJSON = {
        "lineData": {
            line_id: create_line_data(
                start_date=start_date,
                end_date=end_date,
                service_levels=service_level_entries[line_id],
                ridership=ridership_entries[line_id],
            )
            for line_id in service_level_entries.keys()
        },
    }
    with open("dash.json", "w") as f:
        json.dump(dash_json, f)


if __name__ == "__main__":
    create_service_ridership_dash_json()
