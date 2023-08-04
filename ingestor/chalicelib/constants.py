from datetime import datetime, timedelta
from decimal import Decimal
from chalicelib import stations

ALL_ROUTES = [
    ["line-red", "a"],
    ["line-red", "b"],
    ["line-orange", None],
    ["line-blue", None],
    ["line-green", "b"],
    ["line-green", "c"],
    ["line-green", "d"],
    ["line-green", "e"],
]
STATIONS = stations.STATIONS

TERMINI_NEW = {
    "line-red": {
        "a": {
            "line": "line-red",
            "route": "a",
            "stops": [
                [STATIONS["SHAWMUT"]["NB"], STATIONS["DAVIS"]["NB"]],
                [STATIONS["DAVIS"]["SB"], STATIONS["SHAWMUT"]["SB"]],
            ],
            "length": Decimal("20.26"),
        },
        "b": {
            "line": "line-red",
            "route": "b",
            "stops": [
                [STATIONS["QUINCY_ADAMS"]["NB"], STATIONS["DAVIS"]["NB"]],
                [STATIONS["DAVIS"]["SB"], STATIONS["QUINCY_ADAMS"]["SB"]],
            ],
            "length": Decimal("29.64"),
        },
    },
    "line-orange": {
        "line": "line-orange",
        "route": None,
        "stops": [
            [STATIONS["GREEN_STREET"]["NB"], STATIONS["MALDEN_CENTER"]["NB"]],
            [STATIONS["MALDEN_CENTER"]["SB"], STATIONS["GREEN_STREET"]["SB"]],
        ],
        "length": Decimal("19.22"),
    },
    "line-blue": {
        "line": "line-blue",
        "route": None,
        "stops": [
            [STATIONS["GOV_CENTER_BLUE"]["NB"], STATIONS["REVERE_BEACH"]["NB"]],
            [STATIONS["REVERE_BEACH"]["SB"], STATIONS["GOV_CENTER_BLUE"]["SB"]],
        ],
        "length": Decimal("10.75"),
    },
    "line-green-post-glx": {
        "b": {
            "line": "line-green",
            "route": "b",
            "stops": [
                [STATIONS["SOUTH_ST"]["NB"], STATIONS["BOYLSTON"]["NB"]],
                [STATIONS["BOYLSTON"]["SB"], STATIONS["SOUTH_ST"]["SB"]],
            ],
            "length": Decimal("5.39") * 2,
        },
        "c": {
            "line": "line-green",
            "route": "c",
            "stops": [
                [STATIONS["ENGLEWOOD"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["ENGLEWOOD"]["SB"]],
            ],
            "length": Decimal("4.91") * 2,
        },
        "d": {
            "line": "line-green",
            "route": "d",
            "stops": [
                [STATIONS["WOODLAND"]["NB"], STATIONS["LECHMERE"]["NB"]],
                [STATIONS["LECHMERE"]["SB"], STATIONS["WOODLAND"]["SB"]],
            ],
            "length": Decimal("12.81") * 2,
        },
        "e": {
            "line": "line-green",
            "route": "e",
            "stops": [
                [STATIONS["BACK_OF_THE_HILL"]["NB"], STATIONS["BALL_SQ"]["NB"]],
                [STATIONS["BALL_SQ"]["SB"], STATIONS["BACK_OF_THE_HILL"]["SB"]],
            ],
            "length": Decimal("7.88") * 2,
        },
    },
    "line-green-pre-glx": {
        "b": {
            "line": "line-green",
            "route": "b",
            "stops": [
                [STATIONS["SOUTH_ST"]["NB"], STATIONS["BOYLSTON"]["NB"]],
                [STATIONS["BOYLSTON"]["SB"], STATIONS["SOUTH_ST"]["SB"]],
            ],
            "length": Decimal("5.39") * 2,
        },
        "c": {
            "line": "line-green",
            "route": "c",
            "stops": [
                [STATIONS["ENGLEWOOD"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["ENGLEWOOD"]["SB"]],
            ],
            "length": Decimal("4.91") * 2,
        },
        "d": {
            "line": "line-green",
            "route": "d",
            "stops": [
                [STATIONS["WOODLAND"]["NB"], STATIONS["PARK_ST"]["NB"]],
                [STATIONS["PARK_ST"]["SB"], STATIONS["WOODLAND"]["SB"]],
            ],
            "length": Decimal("11.06") * 2,
        },
        "e": {
            "line": "line-green",
            "route": "e",
            "stops": [
                [STATIONS["BACK_OF_THE_HILL"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["BACK_OF_THE_HILL"]["SB"]],
            ],
            "length": Decimal("3.73") * 2,
        },
    },
}


def get_route_metadata(line, date, route=None):
    if line == "line-green":
        if date < GLX_EXTENSION_DATE:
            return TERMINI_NEW[f"{line}-pre-glx"][route]
        return TERMINI_NEW[f"{line}-post-glx"][route]
    if route:
        return TERMINI_NEW[line][route]
    return TERMINI_NEW[line]


LINES = ["line-red", "line-orange", "line-blue", "line-green"]
RIDERSHIP_KEYS = {
    "line-red": "line-Red",
    "line-orange": "line-Orange",
    "line-blue": "line-Blue",
    "line-green": "line-Green",
}
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"
GLX_EXTENSION_DATE = datetime.strptime("2023-03-19", DATE_FORMAT_BACKEND)
TODAY = datetime.now().date()

ONE_WEEK_AGO_STRING = (TODAY - timedelta(weeks=1)).strftime(DATE_FORMAT_BACKEND)
NINETY_DAYS_AGO_STRING = (TODAY - timedelta(days=90)).strftime(DATE_FORMAT_BACKEND)


DD_URL_AGG_TT = "https://dashboard-api.labs.transitmatters.org/api/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api.labs.transitmatters.org/api/traveltimes/{date}?{parameters}"


def get_monthly_table_update_start():
    """Get 1st of current month"""
    yesterday = datetime.today() - timedelta(days=1)
    first_of_month = datetime(yesterday.year, yesterday.month, 1)
    return first_of_month


def get_weekly_table_update_start():
    """Get Sunday of current week."""
    yesterday = datetime.now() - timedelta(days=1)
    days_since_monday = yesterday.weekday() % 7
    most_recent_monday = yesterday - timedelta(days=days_since_monday)
    return most_recent_monday


# Configuration for aggregate speed table functions
TABLE_MAP = {
    "weekly": {
        "table_name": "DeliveredTripMetricsWeekly",
        "start_date": datetime.strptime("2016-01-11T08:00:00", DATE_FORMAT),  # Start on first Monday with data.
        "update_start": get_weekly_table_update_start(),
    },
    "monthly": {
        "table_name": "DeliveredTripMetricsMonthly",
        "start_date": datetime.strptime("2016-01-01T08:00:00", DATE_FORMAT),  # Start on 1st of first month with data.
        "update_start": get_monthly_table_update_start(),
    },
}


LINE_TO_ROUTE_MAP = {
    "line-red": ["line-red-a", "line-red-b"],
    "line-green": ["line-green-b", "line-green-c", "line-green-d", "line-green-e"],
    "line-blue": ["line-blue"],
    "line-orange": ["line-orange"],
}
