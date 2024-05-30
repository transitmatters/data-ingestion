from itertools import chain
import sys
from chalicelib import s3
from botocore.exceptions import ClientError

ROUTE_DEFINITIONS = {
    "Red": {"labels": range(1900, 2152), "core_stations": [70077, 70078]},  # Downtown Crossing
    "Orange": {"labels": range(1400, 1552), "core_stations": [70014, 70015]},  # Back Bay
    "Green": {"labels": range(3900, 3923), "core_stations": [70200, 70196]},  # Park Street
}

EVENT_DEPARTURE = ["DEP", "PRD"]

BUCKET = "tm-mbta-performance"
KEY = "NewTrains/run_counts/{}.csv"


# Handle dual green line cars, we don't need to care about both, just grab the first
def parse_vehicle_label(label):
    if "-" in label:
        return label.split("-")[0]
    return label


def train_runs(route, date):
    spec = ROUTE_DEFINITIONS[route]
    api_data = (
        []
    )  # TODO: This used to be a call to MbtaPerformanceAPI.get_train_data, but that function no longer exists
    events = sum([stop["events"] for stop in api_data], [])
    departures = filter(lambda event: event["event_type"] in EVENT_DEPARTURE, events)
    by_trip_id = {event["trip_id"]: event for event in departures}  # Just in case a single trip gets a DEP and a PRD
    return list(
        filter(lambda event: int(parse_vehicle_label(event["vehicle_label"])) in spec["labels"], by_trip_id.values())
    )


def unique_trains(train_events):
    # Using | as a delimeter as to not create an undefined amount of columns in a csv
    train_list = list(set(list(chain.from_iterable([event["vehicle_label"].split("-") for event in train_events]))))
    return "|".join(train_list)


def update_all(date):
    for route in ROUTE_DEFINITIONS.keys():
        print(f"Storing new train runs for {route}...")
        try:
            train_events = train_runs(route, date)
            run_count = len(train_events)
            unique_train_runs = unique_trains(train_events)
            update_statistics_file(route, date, run_count, unique_train_runs)
        except Exception:
            print(f"Unable to store new train run count for route={route}", file=sys.stderr)
            print(sys.exc_info()[2], file=sys.stderr)
            continue


def update_statistics_file(route, date, count, unique_train_runs):
    csv_row = "{formatted_date},{count},{unique_train_runs}\n".format(
        formatted_date=date.strftime("%Y-%m-%d"), count=count, unique_train_runs=unique_train_runs
    )
    key = KEY.format(route)
    try:
        data = s3.download(BUCKET, key, compressed=False) + csv_row
    except ClientError as ex:
        if ex.response["Error"]["Code"] != "NoSuchKey":
            raise
        data = "service_date,run_count\n" + csv_row

    s3.upload(BUCKET, key, data.encode(), compress=False)
