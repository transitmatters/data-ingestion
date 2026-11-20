"""What the home page measures and how it's judged.

Mirrors the compute half of the dashboard's modules/home/config.ts. Labels, definitions and the
order of rows and hero tiles stay in the dashboard, which joins them to this data by id.
"""

from typing import TypedDict

# Thresholds are (win, good, room to grow), read in the metric's direction: a score that reaches
# "win" is a Win, one that reaches "good" is Good, one that reaches "room" is Room to grow, and
# anything short of that Needs work. Agreed with TM leadership in t-performance-dash#1211.
METRICS = {
    "service": {"direction": "higher", "thresholds": (1, 0.92, 0.8), "score_unit": "pct", "stale_days": 21},
    "frequency": {"direction": "higher", "thresholds": (1, 0.9, 0.75), "score_unit": "pct", "stale_days": 21},
    "speed": {"direction": "higher", "thresholds": (1, 0.95, 0.88), "score_unit": "pct", "stale_days": 21},
    # Share of headways within 75-125% of the scheduled headway. Judged on complete weeks only.
    "reliability": {"direction": "higher", "thresholds": (0.8, 0.65, 0.5), "score_unit": "pct", "stale_days": 10},
    # Share of the service day with a delay alert up.
    "delays": {"direction": "lower", "thresholds": (0.02, 0.04, 0.08), "score_unit": "pct", "stale_days": 10},
}

METRIC_ORDER = ["service", "frequency", "reliability", "delays", "speed"]

# Commuter Rail's Reliability cell is the MBTA's on-time share, not headways. "Good" is the MBTA's
# 92% target. The MBTA publishes in batches months apart, so data stays usable for longer.
CR_RELIABILITY = {"direction": "higher", "thresholds": (0.95, 0.92, 0.85), "score_unit": "pct", "stale_days": 150}

# Slow zones have no column of their own: subway Speed takes the worse of speed and slow zones.
SLOW_ZONES = {"direction": "lower", "thresholds": (30, 60, 180), "score_unit": "seconds", "stale_days": 7}

# Headway goals in minutes. Commuter Rail has none: the MBTA hasn't committed to one for the whole
# system (Full T Ahead only names the inner core and busier corridors).
FREQUENCY_GOALS = {"subway": 10, "frequentBus": 15, "bus": 15}

# Complete weeks in the status window.
WINDOW_WEEKS = 12
# Weeks with data needed before a cell gets a status.
MIN_WEEKS = 8

# The Frequent Bus row is every service & ridership line with a route the MBTA describes as
# "Frequent Bus" (GTFS route_desc). This is that list as of Nov 2026, used when the MBTA's API can't
# be reached. SL1–3 and SLW are one line there, SL4/SL5 another, and 15 rides with 171.
FALLBACK_FREQUENT_BUS_LINE_IDS = [
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
    {"id": "red", "label": "Red Line", "group": "core", "line": "line-red", "href": "/red", "minMeasured": 3},
    {
        "id": "orange",
        "label": "Orange Line",
        "group": "core",
        "line": "line-orange",
        "href": "/orange",
        "minMeasured": 3,
    },
    {"id": "blue", "label": "Blue Line", "group": "core", "line": "line-blue", "href": "/blue", "minMeasured": 3},
    {"id": "green", "label": "Green Line", "group": "core", "line": "line-green", "href": "/green", "minMeasured": 3},
    {
        "id": "commuter-rail",
        "label": "Commuter Rail",
        "group": "core",
        "line": "line-commuter-rail",
        "href": "/commuter-rail/ridership",
        "minMeasured": 2,
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
        "minMeasured": 2,
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
