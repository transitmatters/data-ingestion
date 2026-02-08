import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from decimal import Decimal
from typing import List
from urllib.parse import urlencode

import pandas as pd
import requests

from .. import constants, dynamo
from .types import AggTravelTimesRequest, AggTravelTimesResponse, DirectionType, PeakType

KEYS_TO_KEEP = ["25%", "50%", "75%", "count", "max", "mean", "min", "std"]


def create_dataframe(dicts) -> pd.DataFrame:
    """Create a typed DataFrame from a list of dictionaries.

    Args:
        dicts: A list of dictionaries containing travel time data.

    Returns:
        A pandas DataFrame with enforced column types for travel time metrics.
    """
    return pd.DataFrame(
        dicts,
        {
            "25%": int,
            "50%": int,
            "75%": int,
            "count": int,
            "max": int,
            "mean": float,
            "min": int,
            "std": float,
            "peak": PeakType,
            "service_date": str,
            "from_stop": str,
            "to_stop": str,
            "route_id": str,
            "direction": DirectionType,
            "includes_terminals": bool,
        },
    )


def request_agg_travel_time(request: AggTravelTimesRequest) -> AggTravelTimesResponse:
    """Fetch aggregated travel time data from the Data Dashboard backend API.

    Args:
        request: An AggTravelTimesRequest specifying the route, stops, and date range.

    Returns:
        A list of aggregated travel time entries grouped by date.

    Raises:
        requests.exceptions.HTTPError: If the API returns a non-success status code.
    """
    params = {
        "from_stop": request.stop_pair[0],
        "to_stop": request.stop_pair[1],
        "start_date": date.strftime(request.start_date, constants.DATE_FORMAT_BACKEND),
        "end_date": date.strftime(request.end_date, constants.DATE_FORMAT_BACKEND),
    }
    request_url = constants.DD_URL_AGG_TT.format(parameters=urlencode(params, doseq=True))
    response = requests.get(request_url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(response.content.decode("utf-8"))
        raise
    return json.loads(response.content.decode("utf-8"))


def get_date_ranges(start_date: date, end_date: date, max_range_size: int, breakpoint_dates: List[date] = []):
    """Split a date range into smaller sub-ranges respecting a max size and breakpoints.

    Divides the range [start_date, end_date] into chunks no larger than
    max_range_size days, further splitting at any breakpoint dates that fall
    within a chunk.

    Args:
        start_date: The start of the overall date range.
        end_date: The end of the overall date range.
        max_range_size: The maximum number of days in each sub-range.
        breakpoint_dates: Dates at which ranges must be split (e.g. service changes).

    Returns:
        A list of (start, end) date tuples representing the sub-ranges.
    """
    naive_ranges = []
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=max_range_size)
        if next_date > end_date:
            next_date = end_date
        naive_ranges.append((current_date, next_date))
        current_date = next_date
    ranges = []
    for start, end in naive_ranges:
        breakpoint_dates_in_range = [d for d in breakpoint_dates if start <= d < end]
        boundaries = [start, *breakpoint_dates_in_range, end]
        for i in range(len(boundaries) - 1):
            ranges.append((boundaries[i], boundaries[i + 1]))
    return [
        (start, end - timedelta(days=1)) if idx != len(ranges) - 1 else (start, end)
        for idx, (start, end) in enumerate(ranges)
    ]


def generate_requests(
    start_date: date,
    end_date: date,
    max_date_range_size: int = 50,
) -> List[AggTravelTimesRequest]:
    """Generate all aggregated travel time API requests for a date range.

    Builds requests for every route, direction, and terminal-inclusivity
    combination across all date sub-ranges.

    Args:
        start_date: The start of the date range to query.
        end_date: The end of the date range to query.
        max_date_range_size: Maximum number of days per API request chunk.

    Returns:
        A list of AggTravelTimesRequest objects covering all route/direction combos.
    """
    reqs = []
    date_ranges = get_date_ranges(start_date, end_date, max_date_range_size, [constants.GLX_EXTENSION_DATE])
    for start_date, end_date in date_ranges:
        for line, route in constants.ALL_ROUTES:
            if line.startswith("line-green"):
                continue
            for includes_terminals in (True, False):
                route_metadata = constants.get_route_metadata(line, start_date, includes_terminals, route)
                stop_pairs = route_metadata["stops"]
                for direction in ("0", "1"):
                    request = AggTravelTimesRequest(
                        route_id=f"{line}-{route}" if route else line,
                        includes_terminals=includes_terminals,
                        direction=direction,
                        stop_pair=tuple(stop_pairs[0 if direction == "0" else 1]),
                        start_date=start_date,
                        end_date=end_date,
                    )
                    reqs.append(request)
    return reqs


def load_travel_time_dataframe(
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load aggregated travel time data into a DataFrame using concurrent API requests.

    Args:
        start_date: The start of the date range to fetch.
        end_date: The end of the date range to fetch.

    Returns:
        A DataFrame containing travel time metrics with route and stop metadata.
    """
    reqs = generate_requests(start_date, end_date)
    df_dicts = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(request_agg_travel_time, req): req for req in reqs}
        for future in as_completed(futures):
            result = future.result()
            key = futures[future]
            for by_date_entry in result:
                df_dicts.append(
                    {
                        **by_date_entry,
                        "from_stop": key.stop_pair[0],
                        "to_stop": key.stop_pair[1],
                        "route_id": key.route_id,
                        "direction": key.direction,
                        "includes_terminals": key.includes_terminals,
                    }
                )
    return pd.DataFrame(df_dicts)


def get_df_entry_for_direction_and_exclusivity(df: pd.DataFrame, direction: DirectionType, includes_terminals: bool):
    """Extract a single row from the DataFrame for a given direction and terminal inclusivity.

    Filters the DataFrame and returns a dictionary of statistical keys prefixed
    with the direction and inclusivity label.

    Args:
        df: A DataFrame of travel time data for a single route and date.
        direction: The direction to filter on ("0" or "1").
        includes_terminals: Whether to filter for terminal-inclusive data.

    Returns:
        A dictionary of prefixed metric keys and values, or None if no matching
        row exists.
    """
    entry = df[(df["direction"] == direction) & (df["includes_terminals"] == includes_terminals)]
    prefix = f"dir_{direction}_{'inclusive' if includes_terminals else 'exclusive'}"
    if entry.empty:
        # Sometimes we don't have any data including terminals for a given date
        return None
    assert len(entry) == 1
    return {f"{prefix}_{key}": value for key, value in entry.iloc[0].to_dict().items() if key in KEYS_TO_KEEP}


def prepare_dict_for_dynamo(row_dict):
    """Convert numeric values in a dictionary to Decimal types for DynamoDB compatibility.

    Args:
        row_dict: A dictionary of metric data with potential float and int values.

    Returns:
        A new dictionary with floats rounded to 2 decimal places and converted
        to Decimal, ints converted to Decimal, and other values unchanged.
    """
    res = {}
    for key, value in row_dict.items():
        if isinstance(value, float):
            res[key] = Decimal(str(round(value, 2)))
        elif isinstance(value, int):
            res[key] = Decimal(value)
        else:
            res[key] = value
    return res


def ingest_trip_metrics(start_date: date, end_date: date):
    """Fetch, transform, and write trip metrics to DynamoDB for a date range.

    Loads travel time data, filters to the "all" peak period, aggregates
    metrics by route and date across directions and terminal inclusivity,
    and batch-writes the results to the DeliveredTripMetricsExtended table.

    Args:
        start_date: The start of the date range to ingest.
        end_date: The end of the date range to ingest.
    """
    df = load_travel_time_dataframe(start_date, end_date)
    df = df[df["peak"] == "all"]
    # get all dates
    dates = df["service_date"].unique()
    route_ids = df["route_id"].unique()
    row_dicts = []
    for route_id in route_ids:
        for date_str in sorted(dates):
            date_data = df[(df["service_date"] == date_str) & (df["route_id"] == route_id)]
            sb_exclusive = get_df_entry_for_direction_and_exclusivity(date_data, "0", False)
            nb_exclusive = get_df_entry_for_direction_and_exclusivity(date_data, "1", False)
            sb_inclusive = get_df_entry_for_direction_and_exclusivity(date_data, "0", True)
            nb_inclusive = get_df_entry_for_direction_and_exclusivity(date_data, "1", True)
            if sb_exclusive and nb_exclusive and sb_inclusive and nb_inclusive:
                row_dict = prepare_dict_for_dynamo(
                    {
                        "date": date_str,
                        "route": route_id,
                        **sb_exclusive,
                        **nb_exclusive,
                        **sb_inclusive,
                        **nb_inclusive,
                    }
                )
                row_dicts.append(row_dict)
    dynamo.dynamo_batch_write(row_dicts, "DeliveredTripMetricsExtended")


def ingest_recent_trip_metrics(lookback_days: int = 1):
    """Ingest trip metrics for the most recent days.

    Args:
        lookback_days: Number of days before today to start ingesting from.
    """
    end = date.today()
    start = end - timedelta(days=lookback_days)
    ingest_trip_metrics(start, end)
