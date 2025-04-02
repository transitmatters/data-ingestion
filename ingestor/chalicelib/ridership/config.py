import re

RIDERSHIP_BOX_URL = "https://massdot.app.box.com/s/21j0q5di9ewzl0abt6kdh5x8j8ok9964"

RIDERSHIP_BUS_XLSX_REGEX = re.compile(r"Weekly_Bus_Ridership_by_Route_(\d{4})\.(\d{1,2})\.(\d{1,2})", re.I)

RIDERSHIP_SUBWAY_CSV_REGEX = re.compile(r"(\d{4})\.(\d{1,2})\.(\d{1,2}) MBTA Gated Station Validations by line", re.I)

CR_RIDERSHIP_ARCGIS_URL = "https://opendata.arcgis.com/api/v3/datasets/e2635c945f5b47a7923e0ee441b040c8_0/downloads/data?format=csv&spatialRefId=4326&where=1=1"

CR_UPDATE_CACHE_URL = "https://hub.arcgis.com/api/download/v1/items/e2635c945f5b47a7923e0ee441b040c8/csv?redirect=false&layers=0&updateCache=true"
