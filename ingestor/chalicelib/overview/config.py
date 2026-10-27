"""What the home page measures and how it's judged.

Mirrors the compute half of the dashboard's modules/home/config.ts. Labels, definitions and the
order of rows and hero tiles stay in the dashboard, which joins them to this data by id.
"""

from typing import TypedDict

# Thresholds are provisional until TM signs off on them.
METRICS = {
    "service": {"direction": "higher", "thresholds": (1, 0.92, 0.8), "score_unit": "pct", "stale_days": 21},
    "frequency": {"direction": "higher", "thresholds": (1, 0.9, 0.75), "score_unit": "pct", "stale_days": 21},
    "speed": {"direction": "higher", "thresholds": (1, 0.95, 0.88), "score_unit": "pct", "stale_days": 21},
    "slowZones": {"direction": "lower", "thresholds": (30, 60, 180), "score_unit": "seconds", "stale_days": 7},
    "fleet": {"direction": "lower", "thresholds": (15, 25, 35), "score_unit": "years", "stale_days": 21},
}

METRIC_ORDER = ["service", "frequency", "speed", "slowZones", "fleet"]

# Headway goals in minutes.
FREQUENCY_GOALS = {"subway": 10, "frequentBus": 15, "bus": 15, "commuterRail": 30}

# Complete weeks in the status window.
WINDOW_WEEKS = 12
# Weeks with data needed before a cell gets a status.
MIN_WEEKS = 8

# Share of cars (percent, 4-week mean) a type needs to count as still in regular service.
IN_SERVICE_SHARE = 1


class FleetType(TypedDict):
    key: str
    label: str
    years: str


# Oldest to newest. Keep in step with the dashboard's modules/fleet/constants/fleetTypes.ts.
FLEET_TYPES: dict[str, list[FleetType]] = {
    "line-red": [
        {"key": "red1", "label": "Pullman-Standard", "years": "1969–70"},
        {"key": "red2", "label": "UTDC", "years": "1987–89"},
        {"key": "red3", "label": "Bombardier", "years": "1993–94"},
        {"key": "red4", "label": "CRRC", "years": "2019–"},
    ],
    "line-orange": [
        {"key": "orange12", "label": "Hawker Siddeley", "years": "1979–81"},
        {"key": "orange14", "label": "CRRC", "years": "2018–25"},
    ],
    "line-green": [
        {"key": "type7", "label": "Type 7", "years": "1986–97"},
        {"key": "type8", "label": "Type 8", "years": "1999–2008"},
        {"key": "type9", "label": "Type 9", "years": "2018–20"},
        {"key": "type10", "label": "Type 10", "years": "2026–"},
    ],
}

# Lines with a single car type publish no fleet mix, so their build years live here.
SINGLE_TYPE_FLEETS: dict[str, FleetType] = {
    "line-blue": {"key": "blue", "label": "Siemens", "years": "2007–09"},
    "line-mattapan": {"key": "pcc", "label": "PCC", "years": "1945–46"},
}

# Commuter Rail fleet is judged on electrification, which runs the other way.
CR_ELECTRIFIED_SHARE = 0
CR_ELECTRIFIED_THRESHOLDS = (1, 0.5, 0.1)

# Bus fleet is judged on the share of trips run by battery-electric or hybrid buses.
BUS_FLEET_THRESHOLDS = (0.9, 0.75, 0.5)

# GTFS route_desc "Frequent Bus" routes, as grouped in the service & ridership data (SL1–3 and SLW
# are one line there, SL4/SL5 another, 15 rides with 171).
FREQUENT_BUS_LINE_IDS = [
    "line-SLWaterfront",
    "line-SLWashington",
    "line-1",
    "line-9",
    "line-15171",
    "line-22",
    "line-23",
    "line-28",
    "line-31",
    "line-32",
    "line-39",
    "line-57",
    "line-66",
    "line-71",
    "line-73",
    "line-77",
    "line-104",
    "line-109",
    "line-110",
    "line-111",
    "line-116",
]

# The four core lines; Mattapan sits under "Also running".
SUBWAY_FREQUENCY_LINE_IDS = ["line-Red", "line-Orange", "line-Blue", "line-Green"]


class RowConfig(TypedDict):
    id: str
    label: str
    group: str
    line: str
    href: str
    # Rows with fewer measured cells than this read "Limited data" instead of a verdict.
    minMeasured: int


ROWS: list[RowConfig] = [
    {"id": "red", "label": "Red Line", "group": "core", "line": "line-red", "href": "/red", "minMeasured": 4},
    {
        "id": "orange",
        "label": "Orange Line",
        "group": "core",
        "line": "line-orange",
        "href": "/orange",
        "minMeasured": 4,
    },
    {"id": "blue", "label": "Blue Line", "group": "core", "line": "line-blue", "href": "/blue", "minMeasured": 4},
    {"id": "green", "label": "Green Line", "group": "core", "line": "line-green", "href": "/green", "minMeasured": 4},
    {
        "id": "commuter-rail",
        "label": "Commuter Rail",
        "group": "core",
        "line": "line-commuter-rail",
        "href": "/commuter-rail/ridership",
        "minMeasured": 3,
    },
    # Frequency is the promise; service backs it up.
    {
        "id": "frequent-bus",
        "label": "Frequent Bus",
        "group": "core",
        "line": "line-bus",
        "href": "/bus/ridership",
        "minMeasured": 2,
    },
    {
        "id": "all-bus",
        "label": "All Bus",
        "group": "core",
        "line": "line-bus",
        "href": "/bus/ridership",
        "minMeasured": 2,
    },
    {
        "id": "mattapan",
        "label": "Mattapan",
        "group": "also",
        "line": "line-mattapan",
        "href": "/mattapan",
        "minMeasured": 3,
    },
    # Scheduled service is the only measure for ferries so far.
    {
        "id": "ferry",
        "label": "Ferry",
        "group": "also",
        "line": "line-ferry",
        "href": "/ferry/ridership",
        "minMeasured": 1,
    },
]

# The tiles across the top. The dashboard shows its own copy of these labels; they're here so the
# file stands on its own.
HERO = [
    {
        "id": "subwayFrequency",
        "label": "Subway",
        "unit": "pct",
        "detail": f"of service hours at every {FREQUENCY_GOALS['subway']} min or better",
        "direction": "higher",
    },
    {
        "id": "service",
        "label": "Scheduled service",
        "unit": "pct",
        "detail": "of the most trips ever scheduled",
        "direction": "higher",
    },
    {
        "id": "slowZones",
        "label": "Slow zone delay",
        "unit": "minutes",
        "detail": "added per day across the subway, last week",
        "direction": "lower",
    },
    {
        "id": "frequentBus",
        "label": "Frequent Bus",
        "unit": "pct",
        "detail": f"of service hours at every {FREQUENCY_GOALS['frequentBus']} min or better",
        "direction": "higher",
    },
    {
        "id": "crHeadway",
        "label": "Commuter Rail midday",
        "unit": "minutes",
        "detail": "between trains on weekdays",
        "direction": "lower",
    },
]
