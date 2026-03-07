import json
from datetime import date
from decimal import Decimal
from urllib.parse import urlencode

import requests

from . import constants

# Static mapping of car ID ranges to build years.
# Source: roster.transithistory.org, matching transitmatters/new-train-tracker PR #279
CARRIAGE_AGES: dict[str, dict[str, int]] = {
    "Blue": {"0700-0793": 2008},
    "Orange": {
        "1400-1415": 2019,  # Initial pilot batch
        "1416-1429": 2020,
        "1430-1461": 2021,
        "1462-1477": 2022,
        "1478-1505": 2023,
        "1506-1531": 2024,
        "1532-1551": 2025,
    },
    "Red": {
        "1500-1651": 1970,
        "1700-1757": 1988,
        "1800-1885": 1994,
        "1900-1911": 2020,  # Initial pilot batch (2019-2022 deliveries)
        "1912-1917": 2024,  # Oct 23 - March 24
        "1918-1933": 2024,
        "1934-1959": 2025,
    },
    "Green": {
        "3600-3649": 1987,
        "3650-3699": 1988,
        "3700-3719": 1997,
        "3800-3894": 2003,
        "3900-3923": 2019,
    },
    "Mattapan": {"3072-3265": 1946},
}

# Maps route line names to the key used in CARRIAGE_AGES
LINE_KEY_MAP: dict[str, str] = {
    "line-red": "Red",
    "line-orange": "Orange",
    "line-blue": "Blue",
    "line-green": "Green",
    "line-mattapan": "Mattapan",
}

# One representative stop pair per line to fetch single-day travel times.
# We only need consist data, so any stop pair on the line works.
REPRESENTATIVE_STOP_PAIRS: dict[str, tuple[int, int]] = {
    "line-red": (70061, 70063),  # Alewife -> Davis
    "line-orange": (70003, 70035),  # Green Street -> Malden Center
    "line-blue": (70040, 70042),  # Gov Center -> Aquarium
    "line-green": (70206, 70155),  # North Station -> Copley (trunk, all branches)
    "line-mattapan": (70274, 70264),  # Capen St -> Cedar Grove
}


def get_car_build_year(car_id: int, line: str) -> int | None:
    """Look up the build year for a car ID on a given line."""
    line_ages = CARRIAGE_AGES.get(line)
    if not line_ages:
        return None
    for range_str, year in line_ages.items():
        low, high = range_str.split("-")
        if int(low) <= car_id <= int(high):
            return year
    return None


def get_avg_car_age_for_line(current_date: date, line: str) -> Decimal | None:
    """Fetch single-day travel times for a line and compute average car age from consist data."""
    line_key = LINE_KEY_MAP.get(line)
    if not line_key:
        return None

    stop_pair = REPRESENTATIVE_STOP_PAIRS.get(line)
    if not stop_pair:
        return None

    params = urlencode({"from_stop": stop_pair[0], "to_stop": stop_pair[1]})
    date_str = current_date.strftime(constants.DATE_FORMAT_BACKEND)
    url = constants.DD_URL_SINGLE_TT.format(date=date_str, parameters=params)

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch travel times for car age ({line}, {date_str}): {e}")
        return None

    data = json.loads(response.content.decode("utf-8"))

    # Extract unique car IDs from vehicle_consist, falling back to vehicle_label (head car)
    car_ids: set[int] = set()
    for trip in data:
        consist = trip.get("vehicle_consist")
        if consist:
            for car_str in consist.split("|"):
                try:
                    car_ids.add(int(car_str))
                except ValueError:
                    continue
        elif trip.get("vehicle_label"):
            # vehicle_label contains the head car ID; use as fallback
            for car_str in trip["vehicle_label"].split("-"):
                try:
                    car_ids.add(int(car_str))
                except ValueError:
                    continue

    if not car_ids:
        return None

    # Look up build years and compute average age
    build_years: list[int] = []
    for car_id in car_ids:
        year = get_car_build_year(car_id, line_key)
        if year is not None:
            build_years.append(year)

    if not build_years:
        return None

    current_year = current_date.year
    avg_age = current_year - (sum(build_years) / len(build_years))
    return Decimal(str(round(avg_age, 1)))
