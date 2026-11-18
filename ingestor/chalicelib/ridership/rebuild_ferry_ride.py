"""One-off rebuild of the ferry and The RIDE rows in the Ridership table (#255).

Those rows used to be 7-day totals, with holiday weeks missing. The daily ingest now stores average
weekdays, but it only overwrites the weeks its current files cover, and never deletes. This rebuilds
every ferry and RIDE line from the source files and deletes the rows the new method doesn't produce:
- RIDE before 2025-09, from the deprecated RIDE dataset the daily ingest no longer reads.
- Weeks with no weekday service, e.g. the weekend-only F8 ferry.

Usage, from ingestor/:
    uv run python -m chalicelib.ridership.rebuild_ferry_ride          # dry run: prints the changes
    uv run python -m chalicelib.ridership.rebuild_ferry_ride --write  # applies them
"""

import sys

import boto3
from boto3.dynamodb.conditions import Key

from .arcgis import _download_arcgis_csv, ferry_update_cache, ride_update_cache
from .config import FERRY_RIDERSHIP_ARCGIS_URL, THE_RIDE_RIDERSHIP_ARCGIS_URL
from .dynamo import DYNAMO_TABLE_NAME, ingest_ridership_to_dynamo
from .gtfs import get_routes_by_line_id
from .ingest import get_ridership_by_line_id
from .process import format_ferry_data, format_ridership_csv, format_the_ride_data, pre_process_csv

# "MBTA The RIDE Ridership" (daily, 2017-02-11 to 2025-08-29), replaced by the current dataset in #191
OLD_RIDE_RIDERSHIP_ARCGIS_URL = "https://opendata.arcgis.com/api/v3/datasets/e93e4e4820ca4719b3c4134ae0865053_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"


def merge_ride_history(old: list[dict], current: list[dict]) -> list[dict]:
    """The old dataset's weeks before the current dataset starts, then the current dataset."""
    if not current:
        return old
    first_current = min(entry["date"] for entry in current)
    return [entry for entry in old if entry["date"] < first_current] + current


def plan_changes(new_by_line: dict[str, list[dict]], existing_by_line: dict[str, list[dict]]):
    """Rows to delete per line: existing dates the new series doesn't have."""
    return {
        line: sorted(
            {row["date"] for row in existing_by_line.get(line, [])}
            - {entry["date"] for entry in new_by_line.get(line, [])}
        )
        for line in sorted(set(new_by_line) | set(existing_by_line))
    }


def _query_line(table, line_id: str) -> list[dict]:
    items, kwargs = [], {"KeyConditionExpression": Key("lineId").eq(line_id)}
    while True:
        response = table.query(**kwargs)
        items.extend(response["Items"])
        if "LastEvaluatedKey" not in response:
            return items
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def rebuild(write: bool = False):
    ferry_update_cache()
    ride_update_cache()
    ferry_file = _download_arcgis_csv(FERRY_RIDERSHIP_ARCGIS_URL, "Ferry")
    ride_file = _download_arcgis_csv(THE_RIDE_RIDERSHIP_ARCGIS_URL, "The RIDE")
    old_ride_file = _download_arcgis_csv(OLD_RIDE_RIDERSHIP_ARCGIS_URL, "The RIDE (2017-2025)")
    if not (ferry_file and ride_file and old_ride_file):
        raise RuntimeError("Couldn't download every ferry and RIDE file; nothing was changed")

    by_route = {**format_ferry_data(ferry_file), **format_the_ride_data(ride_file)}
    old_ride = format_ridership_csv(
        pre_process_csv(old_ride_file, "Date", None, "Completed_Trips", route_name="RIDE"),
        "Date",
        "Route",
        "Completed_Trips",
    )["RIDE"]
    by_route["RIDE"] = merge_ride_history(old_ride, by_route.get("RIDE", []))

    # Every GTFS ferry line, so one with no weekday ridership (e.g. F8) gets an empty series and its old rows go
    ferry_routes = {line: routes for line, routes in get_routes_by_line_id().items() if line.startswith("line-Boat")}
    new_by_line = get_ridership_by_line_id(by_route, ferry_routes)

    table = boto3.resource("dynamodb").Table(DYNAMO_TABLE_NAME)
    existing_by_line = {line: _query_line(table, line) for line in new_by_line}
    deletes = plan_changes(new_by_line, existing_by_line)

    for line in sorted(new_by_line):
        print(
            f"{line}: {len(existing_by_line[line])} rows -> {len(new_by_line[line])}, "
            f"deleting {len(deletes[line])} {deletes[line][:5]}{'...' if len(deletes[line]) > 5 else ''}"
        )
    if not write:
        print("Dry run; pass --write to apply")
        return

    ingest_ridership_to_dynamo({line: entries for line, entries in new_by_line.items() if entries})
    with table.batch_writer() as batch:
        for line, dates in deletes.items():
            for date in dates:
                batch.delete_item(Key={"lineId": line, "date": date})
    print("Done")


if __name__ == "__main__":
    rebuild(write="--write" in sys.argv[1:])
