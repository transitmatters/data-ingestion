import requests
import json
import pandas as pd
from datetime import timedelta, date
from decimal import Decimal
from urllib.parse import urlencode
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed


from .. import constants
from .. import dynamo
from .types import AggTravelTimesRequest, AggTravelTimesResponse, PeakType, DirectionType

KEYS_TO_KEEP = ["25%", "50%", "75%", "count", "max", "mean", "min", "std"]


def create_dataframe(dicts) -> pd.DataFrame:
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
    entry = df[(df["direction"] == direction) & (df["includes_terminals"] == includes_terminals)]
    prefix = f"dir_{direction}_{'inclusive' if includes_terminals else 'exclusive'}"
    if entry.empty:
        # Sometimes we don't have any data including terminals for a given date
        return None
    assert len(entry) == 1
    return {f"{prefix}_{key}": value for key, value in entry.iloc[0].to_dict().items() if key in KEYS_TO_KEEP}


def prepare_dict_for_dynamo(row_dict):
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
    end = date.today()
    start = end - timedelta(days=lookback_days)
    ingest_trip_metrics(start, end)
