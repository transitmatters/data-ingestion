import json
from datetime import date, datetime, timedelta
from typing import cast
from pathlib import PurePath

from .config import (
    START_DATE,
    TIME_ZONE,
    PRE_COVID_DATE,
)
from .service_levels import get_service_level_entries_by_line_id, ServiceLevelsByDate, ServiceLevelsEntry
from .ridership import ridership_by_line_id, RidershipEntry
from .service_summaries import summarize_weekly_service_around_date
from .util import date_to_string
from .time_series import get_time_series
from .summary import get_summary_data
from .types import ServiceRegimes, LineData, DashJSON, LineKind

parent_dir = PurePath(__file__).parent
debug_file_name = parent_dir / "dash.json"


def get_line_kind(route_ids: list[str], line_id: str) -> LineKind:
    if line_id.startswith("line-Boat"):
        return "boat"
    if any((r for r in route_ids if r.lower().startswith("cr-"))):
        return "regional-rail"
    if line_id.startswith("line-SL"):
        return "silver"
    if line_id in ("line-Red", "line-Orange", "line-Blue", "line-Green"):
        return cast(LineKind, line_id.split("-")[1].lower())
    return "bus"


def create_service_regimes(
    service_levels: ServiceLevelsByDate,
    date: date,
) -> ServiceRegimes:
    return {
        "current": summarize_weekly_service_around_date(
            date=date,
            service_levels=service_levels,
        ),
        "oneYearAgo": summarize_weekly_service_around_date(
            date=date - timedelta(days=365),
            service_levels=service_levels,
        ),
        "baseline": summarize_weekly_service_around_date(
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
        "lineKind": get_line_kind(
            route_ids=service_level_entry.route_ids,
            line_id=service_level_entry.line_id,
        ),
        "ridershipHistory": get_time_series(
            entries=ridership,
            entry_value_getter=lambda entry: entry.ridership,
            start_date=start_date,
            max_end_date=end_date,
        ),
        "serviceHistory": get_time_series(
            entries=service_levels,
            entry_value_getter=lambda entry: round(sum(entry.service_levels)),
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
    write_debug_file: bool = False,
):
    service_level_entries = get_service_level_entries_by_line_id(
        start_date=start_date,
        end_date=end_date,
    )
    ridership_entries = ridership_by_line_id(
        start_date=start_date,
        end_date=end_date,
        line_ids=list(service_level_entries.keys()),
    )
    line_data_by_line_id = {
        line_id: create_line_data(
            start_date=start_date,
            end_date=end_date,
            service_levels=service_level_entries[line_id],
            ridership=ridership_entries[line_id],
        )
        for line_id in service_level_entries.keys()
        if service_level_entries[line_id]
        and ridership_entries[line_id]
        and len(service_level_entries[line_id])
        and len(ridership_entries[line_id])
    }
    summary_data = get_summary_data(
        line_data=list(line_data_by_line_id.values()),
        start_date=start_date,
        end_date=end_date,
    )
    dash_json: DashJSON = {
        "summaryData": summary_data,
        "lineData": line_data_by_line_id,
    }
    if write_debug_file:
        with open(debug_file_name, "w") as f:
            json.dump(dash_json, f)


if __name__ == "__main__":
    create_service_ridership_dash_json(write_debug_file=True)
