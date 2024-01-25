import requests
import json
import geopy.distance
import s3
from keys import YANKEE_API_KEY
from botocore.exceptions import ClientError

BUCKET = "tm-shuttle-positions"
KEY = "yankee/last_shuttle_positions.csv"

def load_bus_positions_from_s3():
    try:
        data = s3.download(BUCKET, KEY, compressed=False)

        return data
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise


def update_shuttles():
    url = "https://api.samsara.com/fleet/vehicles/locations"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {YANKEE_API_KEY}"
    }

    response = requests.get(url, headers=headers)

    buses = json.loads(response.text)["data"]

    last_bus_positions = load_bus_positions_from_s3()
    bus_positions = {}

    print(buses)
    for bus in buses:
        print(bus)
        name = bus["name"]
        long = bus["location"]["longitude"]
        lat = bus["location"]["latitude"]

        last_long = None
        last_lat = None

        if name in last_bus_positions:
            # do calculation of distance
            last_lat = last_bus_positions[name]["latitude"]
            last_long = last_bus_positions[name]["longitude"]

            last_coords = (float(last_lat), float(last_long))
            coords = (float(lat), float(long))

            dist = geopy.distance.geodesic(last_coords, coords).miles
            # persist distance to distance table

        bus_positions[name] = { "latitude": lat, "longitude": long }



