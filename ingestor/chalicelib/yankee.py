from typing import Dict, Optional, Tuple
import requests
import time
import pandas as pd
import json
import boto3
from keys import YANKEE_API_KEY
from botocore.exceptions import ClientError
from datetime import datetime
from tempfile import TemporaryDirectory
import s3
from ddtrace import tracer

from typing import List
from sqlalchemy.orm import Session
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import (
    RoutePattern,
    RoutePatternTypicality,
    Trip,
    ShapePoint,
)

ShapeDict = Dict[str, List[ShapePoint]]

BUCKET = "tm-shuttle-positions"
KEY = "yankee/last_shuttle_positions.csv"
BOSTON_COORDS = (-71.057083, 42.361145)
OSRM_DISTANCE_API = "http://router.project-osrm.org/route/v1/driving/"
METERS_PER_MILE = 0.000621371
SHUTTLE_PREFIX = "Shuttle"


def load_bus_positions():
    try:
        data = s3.download(BUCKET, KEY, compressed=False)
        return data
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise


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
    archive = MbtaGtfsArchive(local_archive_path=TemporaryDirectory().name)
    # s3_bucket=s3.Bucket("tm-gtfs"))
    latest_feed = archive.get_latest_feed()
    latest_feed.download_or_build()
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


def get_shuttle_route_shapes() -> ShapeDict:
    session = get_session_for_latest_feed()
    shape_points = get_shuttle_shapes(session)

    return shape_points


def save_bus_positions(bus_positions):
    now_str = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    print(f"{now_str}: saving bus positions")
    file_name = "bus_positions.csv"
    with open(file_name, "w") as f:
        f.write(json.dumps(bus_positions))

    return


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


def get_driving_distance(old_coords: Tuple[float, float], new_coords: Tuple[float, float]) -> Optional[float]:
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


@tracer.wrap()
def _update_shuttles(last_bus_positions, shuttle_shapes: ShapeDict):
    url = "https://api.samsara.com/fleet/vehicles/locations"

    headers = {"accept": "application/json", "authorization": f"Bearer {YANKEE_API_KEY}"}

    response = requests.get(url, headers=headers)
    buses = json.loads(response.text)["data"]
    print(buses)
    bus_positions = []

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
            print(f"Bus {name} at coordinates ({long}, {lat}) not detected on any route")
            continue

        dist = 0
        for pos in last_bus_positions:
            if pos["name"] == name:
                # do calculation of distance
                last_lat = pos["latitude"]
                last_long = pos["longitude"]

                last_coords = (float(last_long), float(last_lat))

                if last_coords != coords:
                    # accumulate distance traveled
                    dist = get_driving_distance(last_coords, coords)
                    dist += pos["distance_travelled"]
        bus_positions.append(
            {
                "name": name,
                "latitude": lat,
                "longitude": long,
                "distance_travelled": dist,
                "detected_route": detected_route,
            }
        )

    return bus_positions


def update_shuttles():
    last_bus_positions = load_bus_positions()
    shuttle_shapes = get_shuttle_route_shapes()

    updated_positions = _update_shuttles(last_bus_positions, shuttle_shapes)

    save_bus_positions(updated_positions)


# for running locally
if __name__ == "__main__":
    last_bus_positions = []
    shuttle_shapes = get_shuttle_route_shapes()

    for i in range(10000):
        last_bus_positions = _update_shuttles(last_bus_positions, shuttle_shapes)

        df = pd.DataFrame.from_records(last_bus_positions)
        # fig = px.scatter_mapbox(df, lat="latitude", lon="longitude",
        #                 zoom=8,
        #                 height=800,
        #                 size="size",
        #                 color="color",
        #                 width=800)
        # fig.update_layout(mapbox_style="open-street-map")
        # fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        # fig.show()
        save_bus_positions(last_bus_positions)
        time.sleep(60)
