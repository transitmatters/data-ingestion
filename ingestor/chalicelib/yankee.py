import json
from dataclasses import dataclass
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError
from ddtrace import tracer
from geopy import distance
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import RoutePattern, RoutePatternTypicality, ShapePoint, Stop, Trip
from sqlalchemy.orm import Session

from chalicelib import dynamo, s3

from .keys import YANKEE_API_KEY

ShapeDict = Dict[str, List[ShapePoint]]
Coords = Tuple[float, float]

BUCKET = "tm-shuttle-positions"
KEY = "yankee/last_shuttle_positions.csv"
BOSTON_COORDS = (-71.057083, 42.361145)
OSRM_DISTANCE_API = "http://router.project-osrm.org/route/v1/driving/"
METERS_PER_MILE = 0.000621371
SHUTTLE_PREFIX = "Shuttle"
STOP_RADIUS_MILES = 0.1
TIME_FORMAT = "%Y-%m-%d-%H:%M:%S"
SHUTTLE_TRAVELTIME_TABLE = "ShuttleTravelTimes"
# hardcoding this for now to avoid messing with the data dashboard
SHUTTLE_LINE = "line-shuttle"


@dataclass(frozen=True)
class ShuttleTravelTime:
    # line of the trip (for now always line-shuttle)
    line: str
    # route of the trip e.g. Shuttle-AlewifeParkSt
    routeId: str
    date: datetime
    # distance in miles of the trip
    distance_miles: float
    # time in minutes of the trip
    time: float
    # yankee's identifier for the bus that made the trip
    name: str


def load_bus_positions() -> Optional[List[Dict]]:
    """Loads the last known bus positions from S3.

    Returns:
        A list of bus position dicts, or None if no previous positions exist.

    Raises:
        ClientError: If an S3 error other than NoSuchKey occurs.
    """
    try:
        data = s3.download(BUCKET, KEY, compressed=False)
        return json.loads(data)
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
    except Exception:
        print("Failed to get last shuttle positions")
        raise


def get_shuttle_stops(session: Session) -> List[Stop]:
    """Fetches all stops with "Shuttle" in their platform name.

    Args:
        session: A SQLAlchemy session for the GTFS database.

    Returns:
        A list of Stop objects for shuttle platforms.
    """
    return session.query(Stop).filter(Stop.platform_name.contains("Shuttle")).all()


def get_shuttle_shapes(
    session: Session,
) -> ShapeDict:
    """Fetches shape points for all active shuttle diversion route patterns.

    Args:
        session: A SQLAlchemy session for the GTFS database.

    Returns:
        A dict mapping route IDs to their ordered list of ShapePoints.
    """
    route_patterns = (
        session.query(RoutePattern)
        .filter(
            RoutePattern.route_pattern_typicality == RoutePatternTypicality.DIVERSION,
            RoutePattern.route_pattern_id.startswith(SHUTTLE_PREFIX),
        )
        .all()
    )

    print(f"Found {len(route_patterns)} active shuttle route patterns")

    shuttle_shapes: ShapeDict = {}
    for route_pattern in route_patterns:
        if route_pattern is None:
            print(f"Unable to fetch route patttern for route id {route_pattern.route_id}")
            continue

        representative_trip = session.query(Trip).filter(Trip.trip_id == route_pattern.representative_trip_id).first()
        #
        if representative_trip is None:
            print(f"Unable to fetch route patttern for route id {route_pattern.route_id}")
            continue

        print(f"Getting shapes for {route_pattern.route_id}")

        shape_points = (
            session.query(ShapePoint)
            .filter(ShapePoint.shape_id == representative_trip.shape_id)
            .order_by(ShapePoint.shape_pt_sequence)
        ).all()

        shuttle_shapes[route_pattern.route_id] = shape_points

    return shuttle_shapes


def get_session_for_latest_feed() -> Session:
    """Creates a SQLAlchemy session for the latest GTFS feed.

    Downloads or builds the latest GTFS feed from S3 and returns
    a SQLite session for querying it.

    Returns:
        A SQLAlchemy Session connected to the latest GTFS SQLite database.
    """
    s3 = boto3.resource("s3")
    archive = MbtaGtfsArchive(
        local_archive_path=TemporaryDirectory().name,
        s3_bucket=s3.Bucket("tm-gtfs"),
    )
    latest_feed = archive.get_latest_feed()
    latest_feed.download_or_build()
    return latest_feed.create_sqlite_session()


# https://en.wikipedia.org/wiki/Even%E2%80%93odd_rule
def is_in_shape(coords: Tuple[float, float], shape: List[ShapePoint]):
    """Determines if a point is inside a polygon using the even-odd rule.

    Args:
        coords: A (longitude, latitude) tuple for the point to test.
        shape: An ordered list of ShapePoints defining the polygon boundary.

    Returns:
        True if the point is inside or on the boundary of the polygon.
    """
    x = coords[0]
    y = coords[1]

    in_shape = False

    for i in range(len(shape)):
        point_a = shape[i]
        # this is fine because shape[-1] is valid python
        point_b = shape[i - 1]

        (ax, ay) = (point_a.shape_pt_lon, point_a.shape_pt_lat)
        (bx, by) = (point_b.shape_pt_lon, point_b.shape_pt_lat)

        if point_a.shape_pt_lon == x and point_a.shape_pt_lat == coords:
            # point is a corner
            return True

        if (point_a.shape_pt_lat > y) != (point_b.shape_pt_lat > y):
            # high school math class fuck yeah
            slope = (ax - x) * (by - ay) - (bx - ax) * (y - ay)
            if slope == 0:
                # point on boundary
                return True
            if (slope < 0) != (by < ay):
                in_shape = not in_shape

    return in_shape


def save_bus_positions(bus_positions: List[dict]):
    """Saves current bus positions to S3 as JSON.

    Args:
        bus_positions: A list of bus position dicts to persist.
    """
    now_str = datetime.now().strftime(TIME_FORMAT)
    print(f"{now_str}: saving bus positions")

    s3.upload(BUCKET, KEY, json.dumps(bus_positions), compress=False)


def write_traveltimes_to_dynamo(travel_times: List[Optional[ShuttleTravelTime]]):
    """Writes shuttle travel times to DynamoDB in batch.

    Args:
        travel_times: A list of ShuttleTravelTime objects (or None) to write.
            None entries are filtered out before writing.
    """
    row_dicts = []
    for travel_time in travel_times:
        if travel_time:
            row_dicts.append(travel_time.__dict__)
    dynamo.dynamo_batch_write(row_dicts, SHUTTLE_TRAVELTIME_TABLE)


def get_driving_distance(old_coords: Tuple[float, float], new_coords: Tuple[float, float]) -> Optional[float]:
    """Calculates the driving distance between two coordinates.

    Uses the OSRM routing API (http://project-osrm.org/docs/v5.5.1/api/#route-service).

    Args:
        old_coords: A (longitude, latitude) tuple for the starting position.
        new_coords: A (longitude, latitude) tuple for the ending position.

    Returns:
        The driving distance in miles between the coordinate pairs,
        or None if the API request failed.
    """
    # coords must be in order (longitude, latitude)
    url = f"{OSRM_DISTANCE_API}/{old_coords[0]},{old_coords[1]};{new_coords[0]},{new_coords[1]}?overview=false"

    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error getting response from OSRM routing API! Returned non-200 response {response.status_code}")
        return None

    response_json = json.loads(response.text)

    api_code = response_json["code"]
    if api_code != "Ok":
        print(f"Error getting response from OSRM routing API! Returned non-ok response {api_code}")
        return None

    print(response_json)

    return float(response_json["routes"][0]["distance"]) * METERS_PER_MILE


# TODO: this function is doing too much, trying to make it chill
@tracer.wrap()
def _update_shuttles(last_bus_positions: List[Dict], shuttle_shapes: ShapeDict, shuttle_stops: List[Stop]):
    """Fetches live bus positions from Samsara and computes travel times.

    For each bus, determines which shuttle route it is on, detects nearby
    stops, and writes travel times to DynamoDB when a bus arrives at a new stop.

    Args:
        last_bus_positions: Previously recorded bus position dicts from S3.
        shuttle_shapes: A dict mapping route IDs to their shape point lists.
        shuttle_stops: A list of shuttle Stop objects from GTFS.

    Returns:
        A list of updated bus position dicts.

    Raises:
        Exception: If the Samsara API returns a non-200 status or unparseable response.
    """
    url = "https://api.samsara.com/fleet/vehicles/locations"

    headers = {"accept": "application/json", "authorization": f"Bearer {YANKEE_API_KEY}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Received status code {response.status_code} from Samsara bus API. Body: {response.text}")
    try:
        buses = json.loads(response.text)["data"]
    except Exception:
        raise Exception(f"Bus response problematic. We received {json}")

    print(buses)
    bus_positions = []

    travel_times: List[Optional[ShuttleTravelTime]] = []

    for bus in buses:
        name = bus["name"]
        long = bus["location"]["longitude"]
        lat = bus["location"]["latitude"]

        coords = (float(long), float(lat))

        # skip buses that aren't in a shuttle shape
        # TODO(rudiejd): optimize this. there is probably a more efficient way to check if a shape is in
        # any one of a list of polygons. Maybe you can use the the ray method on all of the poly poitns?
        detected_route = None
        for route_id, shape in shuttle_shapes.items():
            if is_in_shape(coords, shape):
                # note: this only detects one route for now
                detected_route = route_id
                break

        if detected_route is None:
            continue

        last_detected_stop_id = -1
        last_update_date = None
        # travel times to write to dynamo
        for pos in last_bus_positions:
            if pos["name"] == name:
                last_detected_stop_id = pos["detected_stop_id"]
                last_update_date = pos["last_update_date"]

        detected_stop_id: int = -1
        for stop in shuttle_stops:
            stop_coords = (stop.stop_lon, stop.stop_lat)
            if distance.geodesic(stop_coords, coords).miles <= STOP_RADIUS_MILES:
                detected_stop_id = int(stop.stop_id)

        # if we're not currently near a stop, use the last stop ID we detected
        if detected_stop_id == -1:
            detected_stop_id = last_detected_stop_id

        # here, we've had the bus arrive at a new stop!
        if detected_stop_id != last_detected_stop_id and last_detected_stop_id != -1:
            # insert into table
            print(f"Bus {name} arrived at stop {detected_stop_id} from stop {last_detected_stop_id}")
            travel_time = maybe_create_travel_time(
                name, detected_route, last_detected_stop_id, detected_stop_id, last_update_date, shuttle_stops
            )
            travel_times.append(travel_time)

        # TODO(rudiejd) use an object to serialize this instead of a dict
        bus_positions.append(
            {
                "name": name,
                "latitude": lat,
                "longitude": long,
                "detected_route": detected_route,
                "detected_stop_id": detected_stop_id,
                "last_update_date": datetime.now(),
            }
        )

    write_traveltimes_to_dynamo(travel_times)

    return bus_positions


def maybe_create_travel_time(
    name: str,
    route_id: str,
    last_detected_stop_id: int,
    detected_stop_id: int,
    last_update_date: Optional[str],
    shuttle_stops: List[Stop],
):
    """Creates a ShuttleTravelTime if the driving distance can be computed.

    Args:
        name: The bus identifier from Yankee/Samsara.
        route_id: The detected shuttle route ID (e.g. "Shuttle-AlewifeParkSt").
        last_detected_stop_id: The stop ID where the bus was previously detected.
        detected_stop_id: The stop ID where the bus has just arrived.
        last_update_date: Timestamp string of the previous detection, or None.
        shuttle_stops: A list of shuttle Stop objects for coordinate lookups.

    Returns:
        A ShuttleTravelTime if successful, or None if the travel time
        could not be computed (missing date, coordinates, or distance).
    """
    # don't write travel times with no start date
    if last_update_date is None:
        print(
            f"Position of bus {name} on {route_id} from {last_detected_stop_id} to {detected_stop_id} had no last update date, cannot create travel time"
        )
        return None

    last_update_datetime = datetime.strptime(last_update_date, TIME_FORMAT)
    update_datetime = datetime.now()

    last_stop_coords: Optional[Coords] = None
    stop_coords: Optional[Coords] = None

    # TODO(rudiejd) this can be made O(1) if it's slow
    for stop in shuttle_stops:
        coords = (stop.stop_lon, stop.stop_lat)
        if stop.stop_id == last_detected_stop_id:
            last_stop_coords = coords
        elif stop.stop_id == detected_stop_id:
            stop_coords = coords

        if stop_coords is not None and last_stop_coords is not None:
            break

    if stop_coords is None or last_stop_coords is None:
        print(
            f"Unable to detect stop ids. Last stop coordinates {last_stop_coords}, current stop coordinates {stop_coords}"
        )
        return None

    # TODO(rudiejd) maybe precompute the stop distances for all the shuttle lines?
    dist = get_driving_distance(last_stop_coords, stop_coords)

    if dist is None:
        print(f"Unable calculate driving distnance for stop ids {last_detected_stop_id}, {detected_stop_id}")
        return None
    # total time in minutes
    time_minutes = (update_datetime - last_update_datetime).total_seconds() // 60

    return ShuttleTravelTime(SHUTTLE_LINE, route_id, datetime.today(), dist, time_minutes, name)


def update_shuttles():
    """Updates shuttle travel times for Yankee Transit shuttles.

    Detects which shuttles have arrived at a new stop since the last run,
    computes driving distances via the OSRM routing API, and writes travel
    times (in minutes) to the ShuttleTravelTimes DynamoDB table.

    Shuttle routes are identified by checking which GTFS shape contains
    the shuttle's position. Previous positions are persisted to S3 to
    track stop-to-stop transitions between invocations.
    """
    last_bus_positions = load_bus_positions()

    if not last_bus_positions:
        last_bus_positions = []

    session = get_session_for_latest_feed()
    shuttle_shapes = get_shuttle_shapes(session)
    shuttle_stops = get_shuttle_stops(session)

    _update_shuttles(last_bus_positions, shuttle_shapes, shuttle_stops)
