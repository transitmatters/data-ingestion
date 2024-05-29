from datetime import date, datetime, timedelta
from decimal import Decimal
from . import stations

ALL_ROUTES: list[tuple[str, str | None]] = [
    ("line-red", "a"),
    ("line-red", "b"),
    ("line-orange", None),
    ("line-blue", None),
    ("line-green", "b"),
    ("line-green", "c"),
    ("line-green", "d"),
    ("line-green", "e"),
]
ALL_LINES: list[str] = ["Red", "Orange", "Blue", "Green-B", "Green-C", "Green-D", "Green-E"]
STATIONS = stations.STATIONS

TERMINI_NEW = {
    "line-red": {
        "a": {
            "line": "line-red",
            "route": "a",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["SHAWMUT"]["NB"], STATIONS["DAVIS"]["NB"]],
                    [STATIONS["DAVIS"]["SB"], STATIONS["SHAWMUT"]["SB"]],
                ],
                "length": Decimal("20.26"),
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["ASHMONT"]["NB"], STATIONS["ALEWIFE"]["NB"]],
                    [STATIONS["ALEWIFE"]["SB"], STATIONS["ASHMONT"]["SB"]],
                ],
            },
        },
        "b": {
            "line": "line-red",
            "route": "b",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["QUINCY_ADAMS"]["NB"], STATIONS["DAVIS"]["NB"]],
                    [STATIONS["DAVIS"]["SB"], STATIONS["QUINCY_ADAMS"]["SB"]],
                ],
                "length": Decimal("29.64"),
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["BRAINTREE"]["NB"], STATIONS["ALEWIFE"]["NB"]],
                    [STATIONS["ALEWIFE"]["SB"], STATIONS["BRAINTREE"]["SB"]],
                ],
            },
        },
    },
    "line-orange": {
        "line": "line-orange",
        "route": None,
        "excluding_terminals": {
            "stops": [
                [STATIONS["GREEN_STREET"]["NB"], STATIONS["MALDEN_CENTER"]["NB"]],
                [STATIONS["MALDEN_CENTER"]["SB"], STATIONS["GREEN_STREET"]["SB"]],
            ],
            "length": Decimal("19.22"),
        },
        "including_terminals": {
            "stops": [
                [STATIONS["FOREST_HILLS"]["NB"], STATIONS["OAK_GROVE"]["NB"]],
                [STATIONS["OAK_GROVE"]["SB"], STATIONS["FOREST_HILLS"]["SB"]],
            ],
        },
    },
    "line-blue": {
        "line": "line-blue",
        "route": None,
        "excluding_terminals": {
            "stops": [
                [STATIONS["GOV_CENTER_BLUE"]["NB"], STATIONS["REVERE_BEACH"]["NB"]],
                [STATIONS["REVERE_BEACH"]["SB"], STATIONS["GOV_CENTER_BLUE"]["SB"]],
            ],
            "length": Decimal("10.75"),
        },
        "including_terminals": {
            "stops": [
                [STATIONS["BOWDOIN"]["NB"], STATIONS["WONDERLAND"]["NB"]],
                [STATIONS["WONDERLAND"]["SB"], STATIONS["BOWDOIN"]["SB"]],
            ],
        },
    },
    "line-green-post-glx": {
        "b": {
            "line": "line-green",
            "route": "b",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["SOUTH_ST"]["NB"], STATIONS["BOYLSTON"]["NB"]],
                    [STATIONS["BOYLSTON"]["SB"], STATIONS["SOUTH_ST"]["SB"]],
                ],
                "length": Decimal("5.39") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["BOSTON_COLLEGE"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["BOSTON_COLLEGE"]["SB"]],
                ]
            },
        },
        "c": {
            "line": "line-green",
            "route": "c",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["ENGLEWOOD"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["ENGLEWOOD"]["SB"]],
                ],
                "length": Decimal("4.91") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["CLEVELAND_CIRCLE"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["CLEVELAND_CIRCLE"]["SB"]],
                ]
            },
        },
        "d": {
            "line": "line-green",
            "route": "d",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["WOODLAND"]["NB"], STATIONS["LECHMERE"]["NB"]],
                    [STATIONS["LECHMERE"]["SB"], STATIONS["WOODLAND"]["SB"]],
                ],
                "length": Decimal("12.81") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["RIVERSIDE"]["NB"], STATIONS["UNION_SQUARE"]["NB"]],
                    [STATIONS["UNION_SQUARE"]["SB"], STATIONS["RIVERSIDE"]["SB"]],
                ]
            },
        },
        "e": {
            "line": "line-green",
            "route": "e",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["BACK_OF_THE_HILL"]["NB"], STATIONS["BALL_SQ"]["NB"]],
                    [STATIONS["BALL_SQ"]["SB"], STATIONS["BACK_OF_THE_HILL"]["SB"]],
                ],
                "length": Decimal("7.88") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["HEATH_ST"]["NB"], STATIONS["MEDFORD_TUFTS"]["NB"]],
                    [STATIONS["MEDFORD_TUFTS"]["SB"], STATIONS["HEATH_ST"]["SB"]],
                ]
            },
        },
    },
    "line-green-pre-glx": {
        "b": {
            "line": "line-green",
            "route": "b",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["SOUTH_ST"]["NB"], STATIONS["BOYLSTON"]["NB"]],
                    [STATIONS["BOYLSTON"]["SB"], STATIONS["SOUTH_ST"]["SB"]],
                ],
                "length": Decimal("5.39") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["BOSTON_COLLEGE"]["NB"], STATIONS["PARK_ST"]["NB"]],
                    [STATIONS["PARK_ST"]["SB"], STATIONS["BOSTON_COLLEGE"]["SB"]],
                ]
            },
        },
        "c": {
            "line": "line-green",
            "route": "c",
            "excluding_terminals": {
                "stops": [
                    [STATIONS["ENGLEWOOD"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["ENGLEWOOD"]["SB"]],
                ],
                "length": Decimal("4.91") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["CLEVELAND_CIRCLE"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["NORTH_STATION_GREEN"]["SB"], STATIONS["NORTH_STATION_GREEN"]["SB"]],
                ]
            },
        },
        "d": {
            "line": "line-green",
            "route": "d",
            "excluding_terminals": {
                "stops": [
                    # Not sure if Park was always the terminus but it is reliable
                    [STATIONS["WOODLAND"]["NB"], STATIONS["PARK_ST"]["NB"]],
                    [STATIONS["PARK_ST"]["SB"], STATIONS["WOODLAND"]["SB"]],
                ],
                "length": Decimal("11.06") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["RIVERSIDE"]["NB"], STATIONS["PARK_ST"]["NB"]],
                    [STATIONS["PARK_ST"]["SB"], STATIONS["RIVERSIDE"]["SB"]],
                ]
            },
        },
        "e": {
            "line": "line-green",
            "route": "e",
            "excluding_terminals": {
                "stops": [
                    # Not sure if Park was always the terminus but it is reliable
                    [STATIONS["BACK_OF_THE_HILL"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["BACK_OF_THE_HILL"]["SB"]],
                ],
                "length": Decimal("3.73") * 2,
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["HEATH_ST"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["HEATH_ST"]["SB"]],
                ]
            },
        },
    },
}


def get_route_metadata(line: str, date: date, include_terminals: bool, route: str | None = None):
    terminals_key = "including_terminals" if include_terminals else "excluding_terminals"
    if line == "line-green":
        if date < GLX_EXTENSION_DATE:
            return TERMINI_NEW[f"{line}-pre-glx"][route][terminals_key]
        return TERMINI_NEW[f"{line}-post-glx"][route][terminals_key]
    if route:
        return TERMINI_NEW[line][route][terminals_key]
    return TERMINI_NEW[line][terminals_key]


LINES: list[str] = ["line-red", "line-orange", "line-blue", "line-green"]
RIDERSHIP_KEYS = {
    "line-red": "line-Red",
    "line-orange": "line-Orange",
    "line-blue": "line-Blue",
    "line-green": "line-Green",
}
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"
GLX_EXTENSION_DATE = datetime.strptime("2023-03-19", DATE_FORMAT_BACKEND).date()
TODAY = datetime.now().date()

ONE_WEEK_AGO_STRING = (TODAY - timedelta(weeks=1)).strftime(DATE_FORMAT_BACKEND)
NINETY_DAYS_AGO_STRING = (TODAY - timedelta(days=90)).strftime(DATE_FORMAT_BACKEND)


DD_URL_AGG_TT = "https://dashboard-api.labs.transitmatters.org/api/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api.labs.transitmatters.org/api/traveltimes/{date}?{parameters}"
DD_URL_ALERTS = "https://dashboard-api.labs.transitmatters.org/api/alerts/{date}?{parameters}"


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
