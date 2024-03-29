from typing import Dict, Optional, Tuple
import requests
import json
import boto3
from .keys import YANKEE_API_KEY
from botocore.exceptions import ClientError
from datetime import datetime
from tempfile import TemporaryDirectory
from geopy import distance
from ddtrace import tracer
from dataclasses import dataclass
from chalicelib import dynamo, s3
from decimal import Decimal

from typing import List
from sqlalchemy.orm import Session
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import RoutePattern, RoutePatternTypicality, Trip, ShapePoint, Stop

ShapeDict = Dict[str, List[ShapePoint]]
Coords = Tuple[float, float]

BUCKET = "tm-shuttle-positions"
KEY = "yankee/last_shuttle_positions.csv"
BOSTON_COORDS = (-71.057083, 42.361145)
MAX_DIST_FROM_BOSTON = 20
OSRM_DISTANCE_API = "http://router.project-osrm.org/route/v1/driving/"
YANKEE_BUS_API = "https://api.samsara.com/fleet/vehicles/locations"
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
    date: str
    # distance in miles of the trip
    distance_miles: Decimal
    # time in minutes of the trip
    time: Decimal
    # ID of the stop from which the shuttle originated
    from_stop_id: str
    # ID of the stop the shuttle travelled to
    to_stop_id: str
    # yankee's identifier for the bus that made the trip
    name: str

@dataclass(frozen=True)
class ShuttlePosition:
    name: str
    latitude: str
    longitude: str
    detected_route: str
    detected_stop_id: str
    last_update_date: str

def load_bus_positions() -> Optional[List[ShuttlePosition]]:
    try:
        data = s3.download(BUCKET, KEY, compressed=False)
        return json.loads(data, object_hook=lambda pos: ShuttlePosition(**pos))
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
    except Exception:
        print("Failed to get last shuttle positions")
        raise


def get_shuttle_stops(session: Session) -> List[Stop]:
    return session.query(Stop).filter(Stop.platform_name.contains("Shuttle")).all()

def get_stop_in_radius(coords: Coords, session: Session) -> Optional[Stop]:
    result: List[Stop] = []

    distance_fn = lambda s: distance.geodesic((s.stop_lon, s.stop_lat), coords) <= STOP_RADIUS_MILES
    try:
        result = session.query(Stop).filter(
            Stop.platform_name.contains("Shuttle"),
        ).all()

        result: List[Stop] = list(filter(distance_fn, result))
    except Exception as e:
        print(f"Failed to match coords {coords} to stop")
        print(f"Exception: {e}")
        return None

    if len(result) == 0:
        return None

    return sorted(result, key=distance_fn)[0]

def get_stop_by_id(session: Session, stop_id: Optional[str]):
    if stop_id is None:
        return None

    result = None
    try:
        result = session.query(Stop).filter(
            Stop.stop_id == stop_id
        ).first()
    except Exception:
        print(f"Failed to find stop with ID {stop_id}")

    return result

# TODO(rudiejd): Make types for the yankee API response
def query_yankee_bus_api(): 
    headers = {"accept": "application/json", "authorization": f"Bearer {YANKEE_API_KEY}"}

    response = requests.get(YANKEE_BUS_API, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Received status code {response.status_code} from Samsara bus API. Body: {response.text}")
    try:
        buses = json.loads(response.text)["data"]
        return buses
    except Exception:
        raise Exception(f"Bus response problematic. We received {json}")



def get_shuttle_shapes(
    session: Session,
) -> ShapeDict:
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
    s3 = boto3.resource("s3")
    archive = MbtaGtfsArchive(
        local_archive_path=TemporaryDirectory().name,
        s3_bucket=s3.Bucket("tm-gtfs"),
    )
    feeds = archive.get_all_feeds()
    if not feeds:
        raise Exception("Failed to get feeds from MBTA list")

    latest_feed = next(feed for feed in reversed(feeds) if feed.exists_remotely())

    if not latest_feed:
        raise Exception("Unable to find feed in S3, aborting")

    print(f"Downloading data from feed with key {latest_feed.key}")

    latest_feed.download_from_s3()

    print("Finished downloading data for feed")

    return latest_feed.create_sqlite_session()


# https://en.wikipedia.org/wiki/Even%E2%80%93odd_rule
def is_in_shape(coords: Tuple[float, float], shape: List[ShapePoint]):
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


def save_bus_positions(bus_positions: List[ShuttlePosition]):
    now_str = datetime.now().strftime(TIME_FORMAT)
    print(f"{now_str}: saving bus positions")

    bus_positions_dicts = list(map(lambda pos: pos.__dict__, bus_positions))

    s3.upload(BUCKET, KEY, json.dumps(bus_positions_dicts), compress=False)


def write_traveltimes_to_dynamo(travel_times: List[ShuttleTravelTime]):
    row_dicts = list(map(lambda pos: pos.__dict__, travel_times))

    print(f"Writing {len(row_dicts)} travel times to dynamo")
    dynamo.dynamo_batch_write(row_dicts, SHUTTLE_TRAVELTIME_TABLE)
    print("Finished writing to dynamo")


def get_driving_distance(old_coords: Tuple[float, float], new_coords: Tuple[float, float]) -> Optional[float]:
    """
    Calculates the driving distance between two coordinates
    Args:
        old_coords: first position
        new_coords: second position

    Returns:
        The distance in miles between the coordinate pairs, or None if the request failed

    Uses the API from http://project-osrm.org/docs/v5.5.1/api/#route-service

    Example response from API:
        (there's also some other stuff we can ignore)
    ```json
    {
      "code": "Ok",
      "routes": [
        {
          "legs": [
            {
              "steps": [],
              "summary": "",
              "weight": 263.2,
              "duration": 260.3,
              "distance": 1886.8
            },
            {
              "steps": [],
              "summary": "",
              "weight": 370.4,
              "duration": 370.4,
              "distance": 2845.4
            }
          ],
          "weight_name": "routability",
          "weight": 633.599999999,
          "duration": 630.7,
          "distance": 4732.2
        }
    }
    ```json
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
def _update_shuttles(last_bus_positions: List[ShuttlePosition], shuttle_shapes: ShapeDict, session: Session):
    buses = query_yankee_bus_api()

    bus_positions: List[ShuttlePosition] = []
    travel_times: List[ShuttleTravelTime] = []

    for bus in buses:
        name = bus["name"]
        long = bus["location"]["longitude"]
        lat = bus["location"]["latitude"]

        coords = (float(long), float(lat))

        # skip all of the buses that are far from boston
        if distance.geodesic(BOSTON_COORDS, coords).miles > MAX_DIST_FROM_BOSTON:
            continue

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

        print(f"Detected bus {name} on route {detected_route} at {long}, {lat}")

        last_detected_stop_id = None
        last_update_date = None

        for pos in last_bus_positions:
            if pos.name == name:
                last_detected_stop_id = pos.detected_stop_id
                last_update_date = pos.last_update_date

        detected_stop: Optional[Stop] = get_stop_in_radius(coords, session)

        if detected_stop is None:
            continue

        print(f"Bus {name} is at stop {detected_stop.stop_name}!")

        # here, we've had the bus arrive at a new stop!
        if detected_stop.stop_id != last_detected_stop_id and last_detected_stop_id != None:
            # insert into table
            print(f"Bus {name} arrived at stop {detected_stop} from stop {last_detected_stop_id}")
            last_detected_stop = get_stop_by_id(session, last_detected_stop_id)
            travel_time = maybe_create_travel_time(
                name, detected_route, last_detected_stop, detected_stop, last_update_date 
            )
            if travel_time:
                travel_times.append(travel_time)


        # Only save the position when it's at a stop
        bus_positions.append(
            ShuttlePosition(name, 
                            lat, 
                            long, 
                            detected_route, 
                            detected_stop.stop_id, 
                            datetime.now().strftime(TIME_FORMAT)))

    write_traveltimes_to_dynamo(travel_times)

    return bus_positions


def maybe_create_travel_time(
    name: str,
    route_id: str,
    last_detected_stop: Optional[Stop],
    detected_stop: Optional[Stop],
    last_update_date: Optional[str],
):
    if last_detected_stop is None or detected_stop is None:
        print(f"Unable to create travel time for stop {last_detected_stop} to {detected_stop}")
        return None

    # don't write travel times with no start date
    if last_update_date is None:
        print(
            f"Position of bus {name} on {route_id} from {last_detected_stop.stop_id} to {detected_stop.stop_id} had no last update date, cannot create travel time"
        )
        return None

    last_update_datetime = datetime.strptime(last_update_date, TIME_FORMAT)
    update_datetime = datetime.now()

    if last_detected_stop is None or detected_stop is None:
        return None

    # TODO(rudiejd) maybe precompute the stop distances for all the shuttle lines?
    dist = get_driving_distance((last_detected_stop.stop_lon, last_detected_stop.stop_lat), (detected_stop.stop_lon, detected_stop.stop_lat))

    if dist is None:
        print(f"Unable calculate driving distnance for stops {last_detected_stop.id}, {detected_stop.stop_id} ({last_detected_stop.stop_name} to {detected_stop.stop_name})")
        return None
    # total time in minutes
    time_minutes = (update_datetime - last_update_datetime).total_seconds() // 60

    return ShuttleTravelTime(SHUTTLE_LINE, 
                             route_id, 
                             datetime.today().strftime("%Y-%m-%d"), 
                             # cover your eyes
                             Decimal(str(round(dist, 2))), 
                             Decimal(str(round(time_minutes, 2))), 
                             last_detected_stop.stop_id,
                             detected_stop.stop_id,
                             name)


def update_shuttles():
    """
    Updates the shuttle travel times table with the travel times, in minutes, of the Yankee
    Transit shuttles that have been detected as arriving at a stop when this lambda is run.
    Shuttle routes are detected by checking the GTFS shape in which the shuttle's position is contained
    in, and distance travelled (driving distance) is calculated by querying the OSRM routing API.

    We persist a record of a shuttle's position to s3 so we know at which station it was previously
    detected.
    """
    last_bus_positions = load_bus_positions()

    if not last_bus_positions:
        last_bus_positions = []

    session = get_session_for_latest_feed()

    print("Finished creating SQLite DB")

    shuttle_shapes = get_shuttle_shapes(session)

    last_bus_positions = _update_shuttles(last_bus_positions, shuttle_shapes, session)

    save_bus_positions(last_bus_positions)

