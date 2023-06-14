from typing import Dict
from mbta_gtfs_sqlite.models import Route

from ..gtfs.utils import bucket_by
from .box import download_latest_ridership_files
from .dynamo import ingest_ridership_to_dynamo
from .gtfs import get_routes_by_line_id
from .process import get_ridership_by_route_id


def get_ridership_by_line_id(
    ridership_by_route_id: Dict[str, Dict],
    routes_by_line_id: Dict[str, Route],
):
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
    return by_line_id


def ingest_ridership_data():
    routes = get_routes_by_line_id()
    subway_file, bus_file = download_latest_ridership_files()
    ridership_by_route_id = get_ridership_by_route_id(subway_file, bus_file)
    ridership_by_line_id = get_ridership_by_line_id(ridership_by_route_id, routes)
    ingest_ridership_to_dynamo(ridership_by_line_id)


if __name__ == "__main__":
    ingest_ridership_data()
