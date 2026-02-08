from typing import Dict

from mbta_gtfs_sqlite.models import Route

from ..gtfs.utils import bucket_by
from .arcgis import cr_update_cache, download_latest_ridership_files, ferry_update_cache, ride_update_cache
from .dynamo import ingest_ridership_to_dynamo
from .gtfs import get_routes_by_line_id
from .process import get_ridership_by_route_id


def get_ridership_by_line_id(
    ridership_by_route_id: Dict[str, Dict],
    routes_by_line_id: Dict[str, Route],
):
    """Aggregate ridership data from route-level to line-level.

    Sums ridership counts across all routes belonging to each line, grouping
    by date. Handles Green Line branch aggregation and adds The RIDE as a
    separate line entry.

    Args:
        ridership_by_route_id: Mapping of route IDs to lists of ridership
            entry dicts with 'date' and 'count' keys.
        routes_by_line_id: Mapping of line IDs to lists of Route objects.

    Returns:
        Mapping of line IDs to sorted lists of ridership entries with
        summed counts per date.
    """
    by_line_id = {}
    for line_id, routes in routes_by_line_id.items():
        route_entries = [entry for route in routes for entry in ridership_by_route_id.get(route.route_id, [])]
        if line_id == "line-Green":
            # Fake this for the green line which is not actually split into branches in the ridership data
            route_entries += ridership_by_route_id.get("Green", [])
        entries_by_date = bucket_by(route_entries, lambda entry: entry["date"])
        summed_entries = [
            {"date": date, "count": sum(entry["count"] for entry in entries)}
            for date, entries in entries_by_date.items()
        ]
        by_line_id[line_id] = sorted(summed_entries, key=lambda entry: entry["date"])

    # Add RIDE data as a separate line since it doesn't have traditional line_id
    if "RIDE" in ridership_by_route_id:
        by_line_id["line-RIDE"] = sorted(ridership_by_route_id["RIDE"], key=lambda entry: entry["date"])

    return by_line_id


def ingest_ridership_data():
    """Run the full ridership ingestion pipeline.

    Downloads the latest ridership files for all transit modes, processes
    and aggregates them by line ID, and writes the results to DynamoDB.
    """
    routes = get_routes_by_line_id()
    cr_update_cache()
    ferry_update_cache()
    ride_update_cache()
    subway_file, bus_file, cr_file, ferry_file, ride_file = download_latest_ridership_files()
    ridership_by_route_id = get_ridership_by_route_id(subway_file, bus_file, cr_file, ferry_file, ride_file)
    ridership_by_line_id = get_ridership_by_line_id(ridership_by_route_id, routes)
    ingest_ridership_to_dynamo(ridership_by_line_id)


if __name__ == "__main__":
    ingest_ridership_data()
