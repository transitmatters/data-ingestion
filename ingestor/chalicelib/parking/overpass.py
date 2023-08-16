from decimal import Decimal
from urllib.error import HTTPError
from urllib.request import urlopen
import osm2geojson
import json


OVERPASS_URL = "http://overpass-api.de/api/interpreter"


def query_overpass(query):
    default_read_chunk_size = 4096

    if not isinstance(query, bytes):
        query = query.encode("utf-8")

    try:
        f = urlopen(OVERPASS_URL, query)
    except HTTPError as e:
        f = e

    response = f.read(default_read_chunk_size)
    while True:
        data = f.read(default_read_chunk_size)
        if len(data) == 0:
            break
        response = response + data
    f.close()

    if f.code == 200:
        if isinstance(response, bytes):
            response = response.decode("utf-8")
        response = json.loads(response)
        geojson = osm2geojson.json2geojson(response)
        return json.dumps(geojson).encode("utf-8")


def query_all_parking():
    all_parking_query = """
    [timeout:60][out:json];
    area[admin_level=4][boundary=administrative][name="Massachusetts"];
    rel(area)[admin_level=8][boundary=administrative];
    map_to_area -> .areas;
    foreach .areas -> .searchArea(
    .searchArea out;
    (
        way["amenity"="parking"](area.searchArea);
    );
    (._;>;);
    out body;
    );
    """
    return query_overpass(all_parking_query)


def query_surface_parking():
    surface_parking_query = """
    [timeout:60][out:json];
    area[admin_level=4][boundary=administrative][name="Massachusetts"];
    rel(area)[admin_level=8][boundary=administrative];
    map_to_area -> .areas;
    foreach .areas -> .searchArea(
    .searchArea out;
    (
        way["amenity"="parking"]["parking"!~"garage"]["building"!~"yes"](area.searchArea);
    );
    (._;>;);
    out body;
    );
    """
    return query_overpass(surface_parking_query)
