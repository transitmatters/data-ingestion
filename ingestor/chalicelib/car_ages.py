import json
from datetime import date
from decimal import Decimal
from urllib.parse import urlencode

import requests

from . import constants

# Static mapping of car ID ranges to build years, used to compute average fleet age.
# Source: roster.transithistory.org, matching transitmatters/new-train-tracker PR #279
CARRIAGE_AGES: dict[str, dict[str, float]] = {
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
        # CRRC delivery schedule below is from the TransitMatters roster PDF (as of Sep 2026),
        # cross-referenced with roster.transithistory.org. Build "year" is rounded to the
        # nearest quarter (.0/.25/.5/.75) since deliveries land throughout the year; adjacent
        # pairs delivered in the same quarter are merged into one range.
        "1900-1911": 2020.5,  # Initial pilot batch, 2019-2022 deliveries
        "1912-1913": 2023.75,  # Q4 2023 (Oct)
        "1914-1917": 2024.0,  # Q1 2024 (Jan-Mar)
        "1930-1931": 2024.25,  # Q2 2024 (Apr)
        "1918-1923": 2024.5,  # Q3 2024 (Jul-Sep)
        "1928-1929": 2024.5,  # Q3 2024 (Aug)
        "1924-1927": 2024.75,  # Q4 2024 (Oct-Nov)
        "1932-1933": 2024.75,  # Q4 2024 (Dec)
        "1934-1939": 2025.0,  # Q1 2025 (Jan-Mar)
        "1940-1945": 2025.25,  # Q2 2025 (Apr-Jun)
        "1946-1953": 2025.5,  # Q3 2025 (Jul-Sep)
        "1954-1957": 2025.75,  # Q4 2025 (Oct)
        "1958-1963": 2026.0,  # Q1 2026 (Jan-Mar)
        "1964-1967": 2026.25,  # Q2 2026 (Apr-May)
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

# Binary new/old car ID ranges, kept in sync with transitmatters/new-train-tracker's
# server/chalicelib/fleet.py (which drives that app's live new/old vehicle toggle).
# Blue and Mattapan have no new (CRRC/CAF Type 9) fleet, so they're omitted here.
NEW_CAR_ID_RANGES: dict[str, tuple[int, int]] = {
    "Red": (1900, 2151),
    "Orange": (1400, 1551),
    "Green": (3900, 3924),
}

# Maps route line names to the key used in CARRIAGE_AGES / NEW_CAR_ID_RANGES
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


def get_car_build_year(car_id: int, line: str) -> float | None:
    """Look up the build year for a car ID on a given line."""
    line_ages = CARRIAGE_AGES.get(line)
    if not line_ages:
        return None
    for range_str, year in line_ages.items():
        low, high = range_str.split("-")
        if int(low) <= car_id <= int(high):
            return year
    return None


def is_car_new(car_id: int, line: str) -> bool:
    """Whether a car ID falls in the new (CRRC / CAF Type 9) fleet range for a line."""
    new_range = NEW_CAR_ID_RANGES.get(line)
    if not new_range:
        return False
    low, high = new_range
    return low <= car_id <= high


def _car_ids_for_trip(trip: dict) -> set[int]:
    """Extract unique car IDs from a trip, preferring the full consist over the head car label."""
    car_ids: set[int] = set()
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
    return car_ids


def get_fleet_age_metrics_for_line(current_date: date, line: str) -> dict[str, Decimal] | None:
    """Fetch a representative day of per-trip consist data for a line and compute:

    - avg_car_age: average age (years) of the unique cars seen that day
    - pct_new_trips: % of trips that day run with at least one new (CRRC/CAF Type 9) car

    Returns None if no consist data is available for the line/date. Either metric may be
    absent from the result if it can't be computed (e.g. no cars matched a known build year).
    """
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

    car_ids: set[int] = set()
    new_trip_count = 0
    total_trip_count = 0
    for trip in data:
        trip_car_ids = _car_ids_for_trip(trip)
        if not trip_car_ids:
            continue
        total_trip_count += 1
        car_ids.update(trip_car_ids)
        if any(is_car_new(car_id, line_key) for car_id in trip_car_ids):
            new_trip_count += 1

    metrics: dict[str, Decimal] = {}

    build_years = [year for car_id in car_ids if (year := get_car_build_year(car_id, line_key)) is not None]
    if build_years:
        # Fractional "now", rounded to the nearest quarter like CARRIAGE_AGES, so a car
        # built earlier this same year doesn't come out with a negative age.
        current_frac_year = current_date.year + ((current_date.month - 1) // 3) * 0.25
        avg_age = current_frac_year - (sum(build_years) / len(build_years))
        metrics["avg_car_age"] = Decimal(str(round(avg_age, 1)))

    if total_trip_count:
        pct_new = (new_trip_count / total_trip_count) * 100
        metrics["pct_new_trips"] = Decimal(str(round(pct_new, 1)))

    return metrics or None
