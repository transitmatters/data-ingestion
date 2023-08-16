from chalicelib.parking.overpass import query_all_parking, query_surface_parking
from chalicelib import s3

BUCKET = "tm-map-data"
KEY = "openstreetmap/parking/{}.geojson"


def ingest_parking_data():
    all_parking_data = query_all_parking()
    key = KEY.format("all_parking")
    s3.upload(BUCKET, key, all_parking_data, compress=False)

    surface_parking_data = query_surface_parking()
    key = KEY.format("surface_parking")
    s3.upload(BUCKET, key, surface_parking_data, compress=False)
