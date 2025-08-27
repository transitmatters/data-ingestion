import pandas as pd
from zoneinfo import ZoneInfo
from .disk import write_event, CSV_FIELDS

EASTERN_TIME = ZoneInfo("US/Eastern")


unofficial_ferry_labels_map = {
    "F1": "Boat-F1",
    "F2H": "Boat-F1",
    "F3": "Boat-EastBoston",
    "F4": "Boat-F4",
    "F5": "Boat-Lynn",
    "F6": "Boat-F6",
    "F7": "Boat-F7",
    "F8": "Boat-F8",
}

inbound_outbound = {"From Boston": 0, "To Boston": 1}

example_field_mapping = {
    "service_date": "service_date",
    "route_id": "route_id",
    "trip_id": "trip_id",
    "direction_id": "travel_direction",
    "stop_id": "stop_id",
    "stop_sequence": None,
    "vehicle_id": "vessel_time_slot",
    "vehicle_label": None,
    "event_type": None,
    "event_time": "actual_arrival",
    "scheduled_headway": "mbta_sched_arrival",
    "scheduled_tt": None,
    "vehicle_consist": None,
}

arrival_field_mapping = {
    "service_date": "service_date",
    "route_id": "route_id",
    "trip_id": "trip_id",
    "travel_direction": "direction_id",
    "arrival_terminal": "stop_id",
    "vessel_time_slot": "vehicle_id",
    "actual_arrival": "event_time",
    "mbta_sched_arrival": "scheduled_headway",
    "scheduled_tt": "scheduled_tt",
}

departure_field_mapping = {
    "service_date": "service_date",
    "route_id": "route_id",
    "trip_id": "trip_id",
    "travel_direction": "direction_id",
    "departure_terminal": "stop_id",
    "vessel_time_slot": "vehicle_id",
    "actual_departure": "event_time",
    "mbta_sched_departure": "scheduled_headway",
    "scheduled_tt": "scheduled_tt",
}

# For these I used context clues from the CSV and then matched up using the MBTA Website to find Stop IDs
station_mapping = {
    "Aquarium": "Boat-Aquarium",
    "Boston": "Boat-Long",
    "Central Whf": "Boat-Aquarium",
    "Georges": "Boat-George",
    "Hingham": "Boat-Hingham",
    "Hull": "Boat-Hull",
    "Lewis": "Boat-Lewis",
    "Logan": "Boat-Logan",
    "Long Wharf N": "Boat-Long",
    "Long Wharf S": "Boat-Long-South",
    "Lynn": "Boat-Blossom",
    "Navy Yard": "Boat-Charlestown",
    "Quincy": "Boat-Quincy",
    "Rowes": "Boat-Rowes",
    "Rowes Wharf": "Boat-Rowes",
    "Seaport": "Boat-Fan",
    "Winthrop": "Boat-Winthrop",
}


def format_ferry_csv(
    path_to_csv_file: str,
):
    # read data, convert to datetime
    df = pd.read_csv(path_to_csv_file, low_memory=False)

    # Calculate Travel time in Minutes
    time_diff = pd.to_datetime(df["mbta_sched_arrival"]) - pd.to_datetime(df["mbta_sched_departure"])
    df["scheduled_tt"] = time_diff.dt.total_seconds() / 60

    # Convert To Boston/From Boston to Inbound/Outbound Values
    df["travel_direction"] = df["travel_direction"].replace(inbound_outbound)
    # Convert direction_id to integer to ensure outputs are integers
    df["travel_direction"] = df["travel_direction"].astype("Int64")
    # Replace terminal values with GTFS Approved Values
    df["departure_terminal"] = df["departure_terminal"].replace(station_mapping)
    df["arrival_terminal"] = df["arrival_terminal"].replace(station_mapping)
    # Replace Route_ids based on mapping
    df["route_id"] = df["route_id"].replace(unofficial_ferry_labels_map)

    # Subset dataframe to just arrival and departure event data - create copies to avoid warnings
    arrival_events = df[arrival_field_mapping.keys()].copy()
    departure_events = df[departure_field_mapping.keys()].copy()

    arrival_events.rename(columns=arrival_field_mapping, inplace=True)
    departure_events.rename(columns=departure_field_mapping, inplace=True)

    # Add missing columns with default values
    for events_df in [arrival_events, departure_events]:
        events_df["stop_sequence"] = None
        events_df["vehicle_label"] = None
        events_df["vehicle_consist"] = None

    # Add event_type to distinguish between arrivals and departures
    arrival_events.loc[:, "event_type"] = "ARR"
    departure_events.loc[:, "event_type"] = "DEP"

    # Convert event_time to datetime, handling mixed formats
    arrival_events.loc[:, "event_time"] = pd.to_datetime(arrival_events["event_time"], format="mixed", errors="coerce")
    departure_events.loc[:, "event_time"] = pd.to_datetime(
        departure_events["event_time"], format="mixed", errors="coerce"
    )

    arrival_events = arrival_events[CSV_FIELDS]
    departure_events = departure_events[CSV_FIELDS]
    events = pd.concat([arrival_events, departure_events])

    return events


def write_events_to_csv(events_dataframe):
    # Filter out events with null or invalid event_time
    events_dataframe = events_dataframe.dropna(subset=["event_time"])
    events = events_dataframe.to_dict(orient="records")
    for event in events:
        write_event(event)
