import json
from datetime import date, datetime, timedelta
from pathlib import PurePath
from typing import Optional, cast

import click

from .config import (
    PRE_COVID_DATE,
    START_DATE,
    TIME_ZONE,
)
from .gtfs import get_routes_by_line
from .ridership import RidershipEntry, ridership_by_line_id
from .s3 import put_dashboard_json_to_s3
from .service_levels import ServiceLevelsByDate, ServiceLevelsEntry, get_service_level_entries_by_line_id
from .service_summaries import summarize_weekly_service_around_date
from .summary import get_summary_data
from .time_series import get_weekly_median_time_series
from .types import DashJSON, LineData, LineKind, ServiceRegimes
from .util import date_from_string, date_to_string

parent_dir = PurePath(__file__).parent
debug_file_name = parent_dir / "dash.json"


def get_line_kind(route_ids: list[str], line_id: str) -> LineKind:
    """Determine the kind of transit line based on route IDs and line ID.

    Args:
        route_ids: A list of route identifiers associated with the line.
        line_id: The MBTA line identifier.

    Returns:
        The LineKind classification (e.g., "bus", "regional-rail", "red").
    """
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
    """Create service regime summaries for current, one year ago, and baseline periods.

    Args:
        service_levels: A dictionary mapping dates to service level entries.
        date: The reference date for computing service regimes.

    Returns:
        A ServiceRegimes dict with current, oneYearAgo, and baseline summaries.
    """
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
    """Build a LineData dictionary containing service and ridership history for a line.

    Args:
        start_date: The start date of the data range.
        end_date: The end date of the data range.
        service_levels: A dictionary mapping dates to service level entries for this line.
        ridership: A dictionary mapping dates to ridership entries for this line.

    Returns:
        A LineData dict with line metadata, ridership history, service history, and regimes.
    """
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
        "ridershipHistory": get_weekly_median_time_series(
            entries=ridership,
            entry_value_getter=lambda entry: entry.ridership,
            start_date=start_date,
            max_end_date=end_date,
        ),
        "serviceHistory": get_weekly_median_time_series(
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
    write_debug_files: bool = False,
    write_to_s3: bool = True,
    include_only_line_ids: Optional[list[str]] = None,
):
    """Generate the complete service ridership dashboard JSON and optionally upload to S3.

    Args:
        start_date: The start date for the dashboard data range.
        end_date: The end date for the dashboard data range.
        write_debug_files: Whether to write a local debug JSON file.
        write_to_s3: Whether to upload the resulting JSON to S3.
        include_only_line_ids: If provided, only include these line IDs in the output.
    """
    print(
        f"Creating service ridership dashboard JSON for {start_date} to {end_date} "
        + f"{'for lines ' + ', '.join(include_only_line_ids) if include_only_line_ids else ''}"
    )
    routes_by_line = get_routes_by_line(include_only_line_ids=include_only_line_ids)
    service_level_entries = get_service_level_entries_by_line_id(
        routes_by_line=routes_by_line,
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
    if write_debug_files:
        with open(debug_file_name, "w") as f:
            json.dump(dash_json, f)
    if write_to_s3:
        put_dashboard_json_to_s3(today=end_date, dash_json=dash_json)


@click.command()
@click.option("--start", default=START_DATE, help="Start date for the dashboard")
@click.option("--end", default=datetime.now(TIME_ZONE).date(), help="End date for the dashboard")
@click.option("--debug", default=False, help="Write debug file", is_flag=True)
@click.option("--s3", default=False, help="Write to S3", is_flag=True)
@click.option("--lines", default=None, help="Include only these line IDs")
def create_service_ridership_dash_json_command(
    start: str,
    end: str,
    debug: bool = False,
    s3: bool = False,
    lines: Optional[str] = None,
):
    """CLI command to create the service ridership dashboard JSON.

    Args:
        start: Start date string for the dashboard.
        end: End date string for the dashboard.
        debug: Whether to write a local debug JSON file.
        s3: Whether to upload the resulting JSON to S3.
        lines: Comma-separated list of line names to include (without "line-" prefix).
    """
    create_service_ridership_dash_json(
        start_date=date_from_string(start),
        end_date=date_from_string(end),
        write_debug_files=debug,
        write_to_s3=s3,
        include_only_line_ids=[f"line-{line}" for line in lines.split(",")] if lines else None,
    )


if __name__ == "__main__":
    create_service_ridership_dash_json_command()
