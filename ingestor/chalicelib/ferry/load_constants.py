import json
import glob
import pandas as pd
import os

# Get the directory where this file is located
current_dir = os.path.dirname(os.path.abspath(__file__))
all_events_files = glob.glob(os.path.join(current_dir, "constants/*.json"))


def load_constants():
    route_dicts = []
    for file_str in all_events_files:
        try:
            with open(file_str, "r") as file:
                data = json.load(file)
                route_dicts.append(crunch_data(data))

        except FileNotFoundError:
            print("Error: 'data.json' not found. Please ensure the file exists.")
        except json.JSONDecodeError:
            print("Error: Could not decode JSON from 'data.json'. Check file format.")
    return route_dicts


def crunch_data(data):
    # Grab first key which represents the route
    route_key = list(data.keys())[0]
    # list of station dicts
    stations = data[route_key]["stations"]

    route_dict = {route_key: {}}

    # Process each direction separately
    for direction in ["0", "1"]:  # outbound, inbound
        route_dict[route_key][direction] = []

        for station in stations:
            # Check if this station has stops for this direction
            if direction in station["stops"]:
                stop_ids = station["stops"][direction]
                order = station["order"]

                # Handle multiple stop IDs for the same station (if any)
                for stop_id in stop_ids:
                    stop_dict = {"stop_id": stop_id, "order": order}
                    route_dict[route_key][direction].append(stop_dict)

    return route_dict


def add_stop_sequence_to_dataframe(df: pd.DataFrame, route_dicts: list) -> pd.DataFrame:
    """
    Add stop_sequence information to a ferry events dataframe based on route_dicts.

    Args:
        df: DataFrame with columns 'route_id', 'direction_id', 'stop_id'
        route_dicts: List of route dictionaries from load_constants()

    Returns:
        DataFrame with stop_sequence column populated
    """
    df_with_sequence = df.copy()

    # Create a mapping dictionary from route_dicts for fast lookup
    stop_sequence_map = {}

    for route_dict in route_dicts:
        for route_id, directions in route_dict.items():
            for direction_id, stops in directions.items():
                for stop_info in stops:
                    key = (route_id, str(direction_id), stop_info["stop_id"])
                    stop_sequence_map[key] = stop_info["order"]

    df_with_sequence["lookup_key"] = list(
        zip(df_with_sequence["route_id"], df_with_sequence["direction_id"].astype(str), df_with_sequence["stop_id"])
    )

    df_with_sequence["stop_sequence"] = df_with_sequence["lookup_key"].map(stop_sequence_map).astype("Int64")

    # Drop the temporary lookup_key column
    df_with_sequence = df_with_sequence.drop("lookup_key", axis=1)

    return df_with_sequence
