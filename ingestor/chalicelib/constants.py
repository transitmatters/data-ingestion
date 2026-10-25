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
    ("line-mattapan", None),
]
ALL_LINES: list[str] = [
    "Red",
    "Orange",
    "Blue",
    "Green-B",
    "Green-C",
    "Green-D",
    "Green-E",
    "Mattapan",
    "CR-Fairmount",
    "CR-NewBedford",
    "CR-Fitchburg",
    "CR-Worcester",
    "CR-Franklin",
    "CR-Greenbush",
    "CR-Haverhill",
    "CR-Kingston",
    "CR-Lowell",
    "CR-Middleborough",
    "CR-Needham",
    "CR-Newburyport",
    "CR-Providence",
]

STATIONS = stations.STATIONS

# Speed is "length" over the travel time between the excluding_terminals stops, so "length" must be
# the distance between those same stops (both directions), not between the terminals.
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
                "length": Decimal("10.152279128638751") * 2,
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
                "length": Decimal("14.831580107795467") * 2,
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
            "length": Decimal("9.605812260150719") * 2,
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
            "length": Decimal("5.387454635553366") * 2,
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
                "length": Decimal("5.371467435799927") * 2,
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
                    [STATIONS["ENGLEWOOD"]["NB"], STATIONS["PARK_ST"]["NB"]],
                    [STATIONS["PARK_ST"]["SB"], STATIONS["ENGLEWOOD"]["SB"]],
                ],
                "length": Decimal("4.613417971546169") * 2,
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
                "length": Decimal("12.768578050435448") * 2,
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
                "length": Decimal("7.869579294420582") + Decimal("7.876516502376123"),
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
                "length": Decimal("5.371467435799927") * 2,
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
                "length": Decimal("4.897437417979001") * 2,
            },
            "including_terminals": {
                "stops": [
                    # Inbound trips arrive on 70205 and outbound trips depart from 70206
                    [STATIONS["CLEVELAND_CIRCLE"]["NB"], STATIONS["NORTH_STATION_GREEN"]["SB"]],
                    [STATIONS["NORTH_STATION_GREEN"]["NB"], STATIONS["CLEVELAND_CIRCLE"]["SB"]],
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
                "length": Decimal("11.022485221928926") * 2,
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
                "length": Decimal("3.6736916227978593") + Decimal("3.6518570572854516"),
            },
            "including_terminals": {
                "stops": [
                    [STATIONS["HEATH_ST"]["NB"], STATIONS["GOV_CENTER_GREEN"]["NB"]],
                    [STATIONS["GOV_CENTER_GREEN"]["SB"], STATIONS["HEATH_ST"]["SB"]],
                ]
            },
        },
    },
    "line-mattapan": {
        "line": "line-mattapan",
        "route": None,
        "excluding_terminals": {
            "stops": [
                [STATIONS["CAPEN_ST"]["NB"], STATIONS["CEDAR_GROVE"]["NB"]],
                [STATIONS["CEDAR_GROVE"]["SB"], STATIONS["CAPEN_ST"]["SB"]],
            ],
            "length": Decimal("1.9374308792899457") * 2,
        },
        "including_terminals": {
            "stops": [
                [STATIONS["MATTAPAN"]["NB"], STATIONS["ASHMONT_MATTAPAN"]["NB"]],
                [STATIONS["ASHMONT_MATTAPAN"]["SB"], STATIONS["MATTAPAN"]["SB"]],
            ],
        },
    },
}


def get_route_metadata(line: str, date: date, include_terminals: bool, route: str | None = None):
    """Retrieves stop and distance metadata for a given route configuration.

    Accounts for the Green Line Extension (GLX) date when selecting
    pre- or post-extension terminus configurations.

    Args:
        line: The line identifier (e.g. "line-red", "line-green").
        date: The service date, used to select pre/post GLX config.
        include_terminals: If True, returns terminal-inclusive stops.
            If False, returns stops excluding terminals with route length.
        route: Optional branch identifier (e.g. "a", "b"). Required for
            lines with branches.

    Returns:
        A dict with "stops" (and optionally "length") for the route.
    """
    terminals_key = "including_terminals" if include_terminals else "excluding_terminals"
    if line == "line-green":
        if date < GLX_EXTENSION_DATE:
            return TERMINI_NEW[f"{line}-pre-glx"][route][terminals_key]
        return TERMINI_NEW[f"{line}-post-glx"][route][terminals_key]
    if route:
        return TERMINI_NEW[line][route][terminals_key]
    return TERMINI_NEW[line][terminals_key]


LINES: list[str] = ["line-red", "line-orange", "line-blue", "line-green", "line-mattapan"]
RIDERSHIP_KEYS = {
    "line-red": "line-Red",
    "line-orange": "line-Orange",
    "line-blue": "line-Blue",
    "line-green": "line-Green",
    "line-mattapan": "line-Mattapan",
}

# Foxborough tends to have irregular service
# Middleborough was replaced by New Bedford on March 24, 2025 (2025-03-24)
COMMUTER_RAIL_LINES = [
    "CR-Fairmount",
    "CR-NewBedford",
    "CR-Fitchburg",
    "CR-Worcester",
    "CR-Franklin",
    "CR-Greenbush",
    "CR-Haverhill",
    "CR-Kingston",
    "CR-Lowell",
    "CR-Middleborough",
    "CR-Needham",
    "CR-Newburyport",
    "CR-Providence",
    "CR-Foxborough",
]

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"
GLX_EXTENSION_DATE = datetime.strptime("2023-03-19", DATE_FORMAT_BACKEND).date()
CR_MIDDLEBOROUGH_DISCONTINUED = datetime.strptime("2025-03-24", DATE_FORMAT_BACKEND).date()
TODAY = datetime.now().date()

ONE_WEEK_AGO_STRING = (TODAY - timedelta(weeks=1)).strftime(DATE_FORMAT_BACKEND)
NINETY_DAYS_AGO_STRING = (TODAY - timedelta(days=90)).strftime(DATE_FORMAT_BACKEND)


DD_URL_AGG_TT = "https://dashboard-api.labs.transitmatters.org/api/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api.labs.transitmatters.org/api/traveltimes/{date}?{parameters}"
DD_URL_ALERTS = "https://dashboard-api.labs.transitmatters.org/api/alerts/{date}?{parameters}"


def commuter_rail_ridership_key(line: str):
    """Converts a commuter rail line ID to its ridership key format.

    Args:
        line: A commuter rail line ID (e.g. "CR-Fairmount").

    Returns:
        The ridership key string (e.g. "line-Fairmount").
    """
    return f"line-{line[3:]}"


def get_monthly_table_update_start():
    """Returns the first day of yesterday's month as a datetime.

    Returns:
        A datetime representing the 1st of the month containing yesterday.
    """
    yesterday = datetime.today() - timedelta(days=1)
    first_of_month = datetime(yesterday.year, yesterday.month, 1)
    return first_of_month


def get_weekly_table_update_start():
    """Returns the most recent Monday relative to yesterday.

    Returns:
        A datetime representing the most recent Monday.
    """
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
    "line-mattapan": ["line-mattapan"],
}

ALERT_PATTERNS = {
    "disabled_vehicle": [
        "disabled train",
        "disabled trolley",
        "train that was disabled",
        "disabled bus",
        "train being taken out of service",
        "train being removed from service",
        "disabled amtrak train",
        "disabled freight train",
    ],
    "signal_problem": [
        "signal problem",
        "signal issue",
        "signal repairs",
        "signal maintenance",
        "signal repair",
        "signal work",
        "signal department",
        "signal personnel",
    ],
    "switch_problem": [
        "switch problem",
        "switch issue",
        "witch problem",
        "witch issue",
        "switching issue",
    ],
    "brake_problem": [
        "brake issue",
        "brake problem",
        "brakes activated",
        "brakes holding",
        "brakes applied",
    ],
    "power_problem": [
        "power problem",
        "power issue",
        "overhead wires",
        "overhead wire",
        "overhear wires",
        "overheard wires",
        "catenary wire",
        "the overhead",
        "wire repair",
        "repairs to the wire",
        "wire maintenance",
        "wire inspection",
        "wire problem",
        "electrical problem",
        "overhead catenary",
        "third rail wiring",
        "power department work",
        "power outage",
        "power maintenance",
        "power equipment",
    ],
    "door_problem": [
        "door problem",
        "door issue",
    ],
    "track_issue": [
        "track issue",
        "track problem",
        "cracked rail",
        "broken rail",
    ],
    "medical_emergency": [
        "medical emergency",
        "ill passenger",
        "medical assistance",
        "medical attention",
        "sick passenger",
        "medical incident",
    ],
    "flooding": [
        "flooding",
    ],
    "police_activity": [
        "police",
    ],
    "fire": [
        "fire",
        "smoke",
        "burning",
    ],
    "mechanical_problem": [
        "mechanical problem",
        "mechanical issue",
        "motor problem",
        "pantograph problem",
        "pantograph issue",
        "issue with the heating system",
        "air pressure problem",
        "train issue",
    ],
    "track_work": [
        "track work",
        "track maintenance",
        "overnight work",
        "track repair",
        "personnel performed maintenance",
        "maintenance work",
        "overnight maintenance",
        "single track",
        "track inspection",
        "inspect the track",
        "maintenance personnel",
        "work crews access",
        "track department personnel",
        "infrastructure repairs",
    ],
    "car_traffic": [
        "unauthorized vehicle on the tracks",
        "vehicle blocking the tracks",
        "auto accident",
        "car on the tracks",
        "car blocking the tracks",
        "car accident",
        "automobile accident",
        "disabled vehicle on the tracks",
        "due to traffic",
        "car in the track area",
        "car blocking the track area",
        "auto that was blocking",
        "auto blocking the track",
        "auto was removed from the track",
        "accident blocking the tracks",
        "road vehicle",
        "vehicle blocking the track",
    ],
}
# Initializing all Delay types at 0 for processing
DELAY_BY_TYPE = {
    "disabled_vehicle": 0,
    "signal_problem": 0,
    "power_problem": 0,
    "door_problem": 0,
    "brake_problem": 0,
    "switch_problem": 0,
    "track_issue": 0,
    "mechanical_problem": 0,
    "track_work": 0,
    "car_traffic": 0,
    "police_activity": 0,
    "medical_emergency": 0,
    "fire": 0,
    "flooding": 0,
    "other": 0,
}
