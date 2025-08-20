from ...ridership.dynamo import ingest_ridership_to_dynamo
from ...ridership.ingest import get_ridership_by_line_id
from ...ridership.process import get_ridership_by_route_id
from ...ridership.gtfs import get_routes_by_line_id
import argparse


"""
For when we need to backfill ridership data with manual file paths.

Example usage:

python -m ingestor.chalicelib.ridership.backfill.manual --subway-file /path/to/subway/file --bus-file /path/to/bus/file --cr-file /path/to/cr/file --ferry-file /path/to/ferry/file

"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ridership data with manual file paths")
    parser.add_argument("--subway-file", required=False, help="Path to subway ridership file")
    parser.add_argument("--bus-file", required=False, help="Path to bus ridership file")
    parser.add_argument("--cr-file", required=False, help="Path to commuter rail ridership file")
    parser.add_argument("--ferry-file", required=False, help="Path to ferry ridership file")

    args = parser.parse_args()

    routes = get_routes_by_line_id()

    ridership_by_route_id = get_ridership_by_route_id(args.subway_file, args.bus_file, args.cr_file, args.ferry_file)
    ridership_by_line_id = get_ridership_by_line_id(ridership_by_route_id, routes)
    ingest_ridership_to_dynamo(ridership_by_line_id)
