import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote, urlencode

import requests

from . import constants, dynamo

DD_API_BASE = "https://dashboard-api.labs.transitmatters.org/api"
DD_URL_AGG_TT_V2 = f"{DD_API_BASE}/aggregate/traveltimes2?{{parameters}}"
BATCH_SIZE = 20


def get_bus_routes():
    """Fetch the list of bus route IDs from the Dashboard API.

    Filters out combined routes (containing '/') since the stops API
    doesn't support them.
    """
    response = requests.get(f"{DD_API_BASE}/routes")
    response.raise_for_status()
    data = response.json()
    routes = data.get("bus", [])
    return [r for r in routes if "/" not in r]


def get_adjacent_stop_pairs(route_id: str):
    """Fetch stops for a bus route and return all adjacent stop pairs for both directions.

    The aggregate travel times API only has data for adjacent stop pairs, not
    endpoint-to-endpoint. Both directions use forward station list order
    (station[i] -> station[i+1]). Some direction 0 pairs may lack data,
    but both directions have data at the route level.
    """
    response = requests.get(f"{DD_API_BASE}/stops/{quote(route_id, safe='')}")
    response.raise_for_status()
    data = response.json()
    stations = data.get("stations", [])

    if len(stations) < 2:
        return []

    stop_pairs = []

    for i in range(len(stations) - 1):
        curr_station = stations[i]
        next_station = stations[i + 1]

        # Direction 0: station[i] -> station[i+1]
        curr_dir0 = curr_station.get("stops", {}).get("0", [])
        next_dir0 = next_station.get("stops", {}).get("0", [])
        if curr_dir0 and next_dir0:
            stop_pairs.append([curr_dir0[0], next_dir0[0]])

        # Direction 1: station[i] -> station[i+1]
        curr_dir1 = curr_station.get("stops", {}).get("1", [])
        next_dir1 = next_station.get("stops", {}).get("1", [])
        if curr_dir1 and next_dir1:
            stop_pairs.append([curr_dir1[0], next_dir1[0]])

    return stop_pairs


def send_agg_tt_v2_requests(stop_pairs, current_date: date):
    """Send requests to the v2 aggregate travel times API and combine results by date.

    The v2 endpoint returns {"by_date": [...], "by_time": [...]}. We use the by_date
    array which has the same fields as the v1 endpoint (count, mean, 50%, etc.).
    """
    delta = timedelta(days=1)
    speed_object = {}

    for stop_pair in stop_pairs:
        params = {
            "from_stop": stop_pair[0],
            "to_stop": stop_pair[1],
            "start_date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(current_date + delta - timedelta(days=1), constants.DATE_FORMAT_BACKEND),
        }
        url = DD_URL_AGG_TT_V2.format(parameters=urlencode(params, doseq=True))
        response = requests.get(url)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            print(response.content.decode("utf-8"))
            raise
        data = json.loads(response.content.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
        by_date = data.get("by_date", [])

        for item in by_date:
            if item["service_date"] in speed_object:
                speed_object[item["service_date"]]["median"] += item["50%"]
                speed_object[item["service_date"]]["mean"] += item["mean"]
                speed_object[item["service_date"]]["count"] += item["count"] / 2
                speed_object[item["service_date"]]["entries"] += 1
            else:
                speed_object[item["service_date"]] = {
                    "median": item["50%"] if item["50%"] else 0,
                    "count": item["count"] / 2 if item["count"] else 0,
                    "mean": item["mean"] if item["mean"] else 0,
                    "entries": 1,
                }

    return speed_object


def process_route(route_id: str, current_date: date):
    """Process a single bus route: fetch adjacent stop pairs, query travel times, return metrics."""
    try:
        stop_pairs = get_adjacent_stop_pairs(route_id)
        if not stop_pairs:
            print(f"No stop pairs for route {route_id}, skipping")
            return None

        speed_object = send_agg_tt_v2_requests(stop_pairs, current_date)

        date_string = current_date.strftime(constants.DATE_FORMAT_BACKEND)
        metrics = speed_object.get(date_string)

        if not metrics:
            print(f"No travel time data for route {route_id} on {date_string}")
            return None

        route_key = f"line-bus-{route_id}"
        result = {
            "route": route_key,
            "line": "line-bus",
            "date": date_string,
            "count": metrics["count"],
            "median": metrics["median"],
            "mean": round(metrics["mean"], 1),
            "total_time": round(metrics["mean"], 1) * metrics["count"],
        }
        return result

    except Exception as e:
        print(f"Error processing route {route_id}: {e}")
        return None


def update_bus_daily_table(current_date: date):
    """Compute and store daily bus trip metrics for all bus routes."""
    print("Fetching bus routes from Dashboard API")
    bus_routes = get_bus_routes()
    print(f"Found {len(bus_routes)} bus routes")

    all_results = []
    date_string = current_date.strftime(constants.DATE_FORMAT_BACKEND)

    # Process routes in batches using ThreadPoolExecutor
    for batch_start in range(0, len(bus_routes), BATCH_SIZE):
        batch = bus_routes[batch_start : batch_start + BATCH_SIZE]
        print(f"Processing batch {batch_start // BATCH_SIZE + 1}: routes {batch[0]}-{batch[-1]}")

        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            futures = {executor.submit(process_route, route_id, current_date): route_id for route_id in batch}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_results.append(result)

    # Write individual route metrics
    if all_results:
        print(f"Writing {len(all_results)} individual bus route metrics")
        dynamo.dynamo_batch_write(all_results, "DeliveredTripMetrics")

    # Compute and write pre-aggregated line-bus row
    if all_results:
        total_count = sum(r["count"] for r in all_results)
        total_time = sum(r["total_time"] for r in all_results)
        agg_row = {
            "route": "line-bus",
            "line": "line-bus",
            "date": date_string,
            "count": total_count,
            "total_time": total_time,
            "miles_covered": None,
        }
        print(f"Writing aggregated line-bus metric: count={total_count}, total_time={total_time}")
        dynamo.dynamo_batch_write([agg_row], "DeliveredTripMetrics")
    else:
        print(f"No bus route data for {date_string}, skipping aggregate row")

    print(f"Bus trip metrics update complete for {date_string}")
