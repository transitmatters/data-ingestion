import requests
import json
import geopy.distance
from keys import YANKEE_API_KEY



def update_shuttles():
    url = "https://api.samsara.com/fleet/vehicles/locations"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {YANKEE_API_KEY}"
    }

    response = requests.get(url, headers=headers)

    buses = json.loads(response.text)["data"]

    bus_positions = {}

    print(buses)
    for bus in buses:
        print(bus)
        name = bus["name"]
        long = bus["location"]["longitude"]
        lat = bus["location"]["latitude"]
        if name not in bus_positions:
            bus_positions[name] = [0]
            continue

