"""Average weekday boardings and alightings at each bus stop, by season.

The MBTA publishes these once a year as "MBTA Bus Ridership by Trip, Season, Route, Line, and Stop", a zip of one
CSV per season with the average riders getting on and off at every stop of every scheduled trip. This sums those
trips into one average weekday per stop and writes one JSON file per GTFS route to S3, which the dashboard's
`/api/bus_stop_ridership` endpoint serves.
"""

import json
import re
import zipfile
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import IO, Dict, Iterator, List, Optional, Tuple

import pandas as pd
import requests

from .. import s3

ARCGIS_ITEM_ID = "8daf4a33925a4df59183f860826d29ee"
DOWNLOAD_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ARCGIS_ITEM_ID}/data"
BUCKET = "tm-mbta-performance"
KEY_PREFIX = "BusStopRidership"

# Only fall seasons are kept: they run every year from 2016, so any two can be compared. The zip also has Spring 2024.
SEASON_FILE_REGEX = re.compile(r"_(Fall)_(\d{4})\.csv$", re.I)

# The column names have changed twice (Fall 2024 and Fall 2025), so each column lists every header it's had.
COLUMN_ALIASES = {
    "route_id": ["route_id", "gtfs route_id"],
    "direction_id": ["direction_id", "gtfs direction_id"],
    "route_variant": ["route_variant", "route/variant"],
    "trip_start_time": ["trip_start_time", "trip start time"],
    "day_type": ["day_type_name", "day type"],
    "stop_id": ["stop_id", "gtfs stop_id"],
    "stop_name": ["stop_name", "stop name"],
    "stop_sequence": ["stop_sequence", "stop sequence"],
    "boardings": ["boardings"],
    "alightings": ["alightings"],
}

# How much each day type counts toward an average weekday. Up to Fall 2024 there's one "weekday" average. From
# Fall 2025 it's split into "Wkdy" (Monday to Thursday, with four times Friday's trip samples) and "Fri".
WEEKDAY_WEIGHTS = {"weekday": 1.0, "wkdy": 4 / 5, "fri": 1 / 5}

CATEGORY_COLUMNS = ["route_id", "direction_id", "route_variant", "trip_start_time", "day_type", "stop_id", "stop_name"]


def download_ridership_zip() -> str:
    """Download the MBTA's bus ridership by stop zip (about 75 MB) to a temporary file.

    Returns:
        The path of the downloaded zip.
    """
    path = NamedTemporaryFile(suffix=".zip", delete=False).name
    with requests.get(DOWNLOAD_URL, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1 << 20):
                file.write(chunk)
    return path


def get_season_name(file_name: str) -> Optional[str]:
    """Get the season a CSV in the zip covers from its file name.

    Args:
        file_name: The CSV's path in the zip, e.g. `.../MBTA_Bus_Ridership_..._Fall_2025.csv`.

    Returns:
        The season, e.g. "Fall 2025", or None for a file that isn't a fall season.
    """
    match = SEASON_FILE_REGEX.search(file_name)
    if not match:
        return None
    return f"{match.group(1).title()} {match.group(2)}"


def read_season_csv(handle: IO[bytes]) -> pd.DataFrame:
    """Read one season's CSV, renaming its columns and keeping only weekday rows.

    Args:
        handle: The CSV, open in binary mode.

    Returns:
        A DataFrame with the columns in COLUMN_ALIASES and a `weight` column from WEEKDAY_WEIGHTS.

    Raises:
        ValueError: If the CSV is missing one of the columns in COLUMN_ALIASES.
    """
    header = pd.read_csv(handle, nrows=0, encoding="utf-8-sig").columns
    handle.seek(0)
    renames = {}
    for column in header:
        for name, aliases in COLUMN_ALIASES.items():
            if column.strip().lower() in aliases:
                renames[column] = name
    missing = set(COLUMN_ALIASES) - set(renames.values())
    if missing:
        raise ValueError(f"Bus ridership CSV is missing columns: {sorted(missing)}")

    df = pd.read_csv(
        handle,
        encoding="utf-8-sig",
        usecols=list(renames),
        # Categories keep a season (about 1.2 million rows) to tens of MB rather than hundreds
        dtype={column: "category" for column, name in renames.items() if name in CATEGORY_COLUMNS},
    ).rename(columns=renames)
    # Mapping a categorical column maps each category once, not each row
    df["weight"] = df["day_type"].map(lambda day_type: WEEKDAY_WEIGHTS.get(str(day_type).strip().lower())).astype(float)
    return df[df["weight"].notna()]


def merge_stop_orders(orders: List[List[str]]) -> List[str]:
    """Merge the stop orders of a route's variants into one order along the route.

    The first order is kept as is. Each stop in the later orders that isn't placed yet goes right after the stop
    before it in its own order, or before the first placed stop if it comes ahead of all of them. Short turns and
    deviations then sit among the stops around them.

    Args:
        orders: Each variant's stop IDs in order, starting with the variant that runs the most trips.

    Returns:
        Every stop ID once, in order.
    """
    merged: List[str] = []
    for order in orders:
        position: Optional[int] = None
        leading: List[str] = []
        for stop_id in order:
            if stop_id in merged:
                index = merged.index(stop_id)
                if leading:
                    merged[index:index] = leading
                    index += len(leading)
                    leading = []
                position = index
            elif position is None:
                leading.append(stop_id)
            else:
                position += 1
                merged.insert(position, stop_id)
        merged.extend(leading)
    return merged


def get_direction_stop_order(direction_df: pd.DataFrame) -> List[str]:
    """Order the stops one direction of a route serves, across all its variants.

    Args:
        direction_df: The direction's rows from read_season_csv.

    Returns:
        Stop IDs in order along the route.
    """
    trips = direction_df.groupby("route_variant", observed=True)["trip_start_time"].nunique()
    orders = []
    for variant in trips.sort_values(ascending=False, kind="stable").index:
        variant_df = direction_df[direction_df["route_variant"] == variant]
        stops = variant_df.drop_duplicates("stop_sequence").sort_values("stop_sequence")["stop_id"]
        orders.append(list(dict.fromkeys(stops)))
    return merge_stop_orders(orders)


def summarize_season(df: pd.DataFrame) -> Iterator[Tuple[str, List[dict]]]:
    """Total one season's average weekday boardings and alightings at each stop, by route and direction.

    Args:
        df: The season's rows from read_season_csv.

    Yields:
        Each route ID with its directions, e.g. `{"direction_id": 0, "stops": [{"stop_id", "boardings",
        "alightings"}, ...]}`, with stops in order along the route.
    """
    df = df.assign(boardings=df["boardings"] * df["weight"], alightings=df["alightings"] * df["weight"])
    for route_id, route_df in df.groupby("route_id", observed=True):
        directions = []
        for direction_id, direction_df in route_df.groupby("direction_id", observed=True):
            totals = direction_df.groupby("stop_id", observed=True)[["boardings", "alightings"]].sum()
            stops = [
                {
                    "stop_id": stop_id,
                    "boardings": round(float(totals.at[stop_id, "boardings"]), 1),
                    "alightings": round(float(totals.at[stop_id, "alightings"]), 1),
                }
                for stop_id in get_direction_stop_order(direction_df)
            ]
            directions.append({"direction_id": int(direction_id), "stops": stops})
        yield route_id, directions


def format_stop_name(csv_name: str) -> str:
    """Tidy a stop name from the CSV, which is upper case and cut off at about 30 characters.

    Args:
        csv_name: The name in the CSV, e.g. "WASHINGTON ST OPP RUGGLES ST".

    Returns:
        The name in title case, e.g. "Washington St opp Ruggles St".
    """
    return re.sub(r"\bOpp\b", "opp", csv_name.strip().title())


def build_route_files(zip_path: str, gtfs_stop_names: Dict[str, str]) -> Dict[str, dict]:
    """Build each route's file from the ridership zip.

    Args:
        zip_path: The path of the zip from download_ridership_zip.
        gtfs_stop_names: Stop names by stop ID from GTFS, which are used ahead of the CSV's names.

    Returns:
        Each route's file by route ID, with its seasons in order and a name for every stop in them.
    """
    routes: Dict[str, dict] = {}
    with zipfile.ZipFile(zip_path) as archive:
        members = [(get_season_name(name), name) for name in archive.namelist()]
        # Seasons in order, so later seasons' CSV stop names win when a stop isn't in GTFS
        for season, name in sorted((m for m in members if m[0]), key=lambda m: int(m[0].split()[1])):
            print(f"Processing bus stop ridership for {season}")
            with archive.open(name) as handle:
                df = read_season_csv(handle)
            for route_id, directions in summarize_season(df):
                route = routes.setdefault(route_id, {"route_id": route_id, "seasons": [], "stop_names": {}})
                route["seasons"].append({"season": season, "directions": directions})
            names = df.drop_duplicates(["route_id", "stop_id"])
            for route_id, stop_id, stop_name in zip(names["route_id"], names["stop_id"], names["stop_name"]):
                routes[route_id]["stop_names"][stop_id] = gtfs_stop_names.get(stop_id) or format_stop_name(stop_name)
            del df
    return routes


def get_gtfs_stop_names() -> Dict[str, str]:
    """Get every stop's name from the latest GTFS feed in S3.

    Returns:
        Stop names by stop ID.
    """
    import boto3
    from mbta_gtfs_sqlite import MbtaGtfsArchive
    from mbta_gtfs_sqlite.models import Stop

    archive = MbtaGtfsArchive(
        local_archive_path=TemporaryDirectory().name,
        s3_bucket=boto3.resource("s3").Bucket("tm-gtfs"),
    )
    feed = archive.get_latest_feed()
    feed.use_compact_only()
    feed.download_or_build()
    session = feed.create_sqlite_session(compact=True)
    return {stop.stop_id: stop.stop_name for stop in session.query(Stop).all()}


def ingest_bus_stop_ridership():
    """Download the MBTA's bus ridership by stop and upload each route's file to S3."""
    routes = build_route_files(download_ridership_zip(), get_gtfs_stop_names())
    for route_id, route in routes.items():
        s3.upload(BUCKET, f"{KEY_PREFIX}/{route_id}.json", json.dumps(route, separators=(",", ":")), compress=False)
    print(f"Uploaded bus stop ridership for {len(routes)} routes")


if __name__ == "__main__":
    ingest_bus_stop_ridership()
