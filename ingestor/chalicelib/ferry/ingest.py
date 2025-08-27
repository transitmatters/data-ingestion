from .arcgis import download_latest_ferry_data, ferry_update_cache
from .process import format_ferry_csv, write_events_to_csv


def ingest_ferry_data():
    ferry_update_cache()
    ferry_file = download_latest_ferry_data()
    events = format_ferry_csv(ferry_file)
    write_events_to_csv(events)


if __name__ == "__main__":
    ingest_ferry_data()
