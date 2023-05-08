import re

RIDERSHIP_BOX_URL = "https://massdot.app.box.com/s/21j0q5di9ewzl0abt6kdh5x8j8ok9964"

RIDERSHIP_BUS_XLSX_REGEX = re.compile(
    r"Weekly_Bus_Ridership_by_Route_(\d{4})\.(\d{1,2})\.(\d{1,2})", re.I
)

RIDERSHIP_SUBWAY_CSV_REGEX = re.compile(
    r"(\d{4})\.(\d{1,2})\.(\d{1,2}) MBTA Gated Station Validations by line", re.I
)
