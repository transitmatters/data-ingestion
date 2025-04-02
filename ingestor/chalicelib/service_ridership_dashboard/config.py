from datetime import date
from pytz import timezone

# Lower bound for time series and GTFS feeds
PRE_COVID_DATE = date(2020, 2, 24)

# Date to use as a baseline
START_DATE = date(2018, 1, 1)

# Boston baby
TIME_ZONE = timezone("US/Eastern")

# Ignore these
IGNORE_LINE_IDS = ["line-CapeFlyer", "line-Foxboro"]

# Date ranges with service gaps that we paper over because of major holidays or catastrophes
# rather than doing more complicated special-casing with GTFS services
FILL_DATE_RANGES = [
    (date(2021, 11, 19), date(2021, 11, 26)),  # Thanksgiving 2021
    (date(2021, 12, 18), date(2021, 12, 26)),  # Christmas 2021
    (date(2022, 12, 18), date(2023, 1, 3)),  # Christmas 2022
    (date(2022, 3, 28), date(2022, 3, 29)),  # Haymarket garage collapse
]
