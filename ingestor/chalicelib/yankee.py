from typing import Optional, Tuple
import requests
import time
import plotly.express as px
import pandas as pd
import json
import geopy.distance
import s3
from keys import YANKEE_API_KEY
from botocore.exceptions import ClientError
from datetime import datetime

BUCKET = "tm-shuttle-positions"
KEY = "yankee/last_shuttle_positions.csv"
BOSTON_COORDS = (42.361145, -71.057083)
OSRM_DISTANCE_API = "http://router.project-osrm.org/route/v1/driving/"

def load_bus_positions():
    file_name = "bus_positions.csv"
    with open(file_name) as f:
        data = f.read()

    js = json.loads(data)
    return js

    # try:
    #     data = s3.download(BUCKET, KEY, compressed=False)
    #
    #     return data
    # except ClientError as ex:
    #     if ex.response["Error"]["Code"] != "NoSuchKey":
    #         raise

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
    The distance in miles between the coordinate pairs

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
    url = f"{OSRM_DISTANCE_API}/{old_coords[0]},{old_coords[1]};{new_coords[0]},{new_coords[1]}?overview=false"

    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error getting response from OSRM routing API! Returned non-200 response {response.status_code}")
        return None

    response_json = json.loads(response.text)

    if response_json["code"] != "Ok":
        print(f"Error getting response from OSRM routing API! Returned non-ok response {response_json["code"]}")
        return None

    return response_json["routes"][0]["distance"]

def update_shuttles(last_bus_positions):
    url = "https://api.samsara.com/fleet/vehicles/locations"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {YANKEE_API_KEY}"
    }

    response = requests.get(url, headers=headers)
    buses = json.loads(response.text)["data"]
    bus_positions = []

    for bus in buses:

        print(bus)

        name = bus["name"]
        long = bus["location"]["longitude"]
        lat = bus["location"]["latitude"]

        coords = (float(lat), float(long))

        # yankee gives us buses all over the place
        # only look at buses < 5 miles from boston 
        if geopy.distance.geodesic(BOSTON_COORDS, coords).miles > 5:
            continue

        last_long = None
        last_lat = None

        if name in last_bus_positions:
            # do calculation of distance
            last_lat = last_bus_positions[name]["latitude"]
            last_long = last_bus_positions[name]["longitude"]

            last_coords = (float(last_lat), float(last_long))

            dist = get_driving_distance(last_coords, coords)
            # persist distance to distance table

            print(f"bus {name} distance travelled: {dist}")
        bus_positions.append({ "name": name, "latitude": lat, "longitude": long, "size": 5, "color": "red"})

    return bus_positions


if __name__ == "__main__":
    last_bus_positions = []
    for i in range(10000):
        last_bus_positions = update_shuttles(last_bus_positions)

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
        time.sleep(60*5)
